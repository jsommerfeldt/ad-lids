# graph_client.py
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional
import requests
from urllib.parse import quote


class GraphClient:
    """
    Thin wrapper around requests.Session for Microsoft Graph calls we need.
    """

    def __init__(self, access_token: str, base: str = "https://graph.microsoft.com/v1.0"):
        self.base = base.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {access_token}"})

    def list_children_by_upn_path(self, upn: str, path: str) -> List[Dict]:
        """
        List items in a folder via path relative to the user's OneDrive root.

        GET /users/{upn}/drive/root:/{path}:/children
        """
        url = f"{self.base}/users/{upn}/drive/root:/{quote(path, safe='/')}:/children"
        items: List[Dict] = []

        resp = self.session.get(url, timeout=30)
        self._raise_for_status(resp)
        data = resp.json()
        items += data.get("value", [])

        # pagination
        while "@odata.nextLink" in data:
            resp = self.session.get(data["@odata.nextLink"], timeout=30)
            self._raise_for_status(resp)
            data = resp.json()
            items += data.get("value", [])

        return items

    def download_item_content(self, drive_id: str, item_id: str) -> bytes:
        """
        Download the file's primary stream by ID.
        GET /drives/{drive-id}/items/{item-id}/content
        """
        url = f"{self.base}/drives/{drive_id}/items/{item_id}/content"
        resp = self.session.get(url, timeout=180)
        self._raise_for_status(resp)
        return resp.content

    def download_item_content_by_user_item(self, upn: str, item_id: str) -> bytes:
        """
        Download a file for a given user using only the item id.
        GET /users/{upn}/drive/items/{item-id}/content
        """
        url = f"{self.base}/users/{upn}/drive/items/{item_id}/content"
        resp = self.session.get(url, timeout=180)
        self._raise_for_status(resp)
        return resp.content

    # ------------- NEW: helpers for path & folder management -------------

    def get_item_by_path(self, upn: str, item_path: str) -> Optional[Dict]:
        """
        Return driveItem metadata for a path relative to the user's Drive root,
        or None if not found.
        GET /users/{upn}/drive/root:/{item_path}
        """
        norm = item_path.strip("/")

        # Root is a special case
        if norm == "":
            url = f"{self.base}/users/{upn}/drive/root"
        else:
            url = f"{self.base}/users/{upn}/drive/root:/{quote(norm, safe='/')}"

        resp = self.session.get(url, timeout=30)
        if resp.status_code == 404:
            return None
        self._raise_for_status(resp)
        return resp.json()

    def ensure_folder_path(self, upn: str, folder_path: str) -> Dict:
        """
        Ensure a (possibly nested) folder path exists under the user's OneDrive root.
        Creates missing segments one by one under:
          POST /users/{upn}/drive/root:/{parent}:/children
          body: { "name": seg, "folder": {}, "@microsoft.graph.conflictBehavior": "fail" }
        Returns driveItem of the final folder.
        """
        folder_path = folder_path.strip("/")
        if folder_path == "":
            # root
            root = self.get_item_by_path(upn, "")
            if root is None:
                raise RuntimeError("Unable to access OneDrive root")
            return root

        parts = [p for p in folder_path.split("/") if p and p != "."]
        parent_path = ""
        parent_item = self.get_item_by_path(upn, "")  # root
        if parent_item is None:
            raise RuntimeError("Unable to access OneDrive root")

        for seg in parts:
            current_path = seg if not parent_path else f"{parent_path}/{seg}"
            existing = self.get_item_by_path(upn, current_path)
            if existing is None:
                # create under parent_path
                parent_for_post = parent_path.strip("/")
                post_url = (
                    f"{self.base}/users/{upn}/drive/root:/"
                    f"{quote(parent_for_post, safe='/')}:/children"
                    if parent_for_post
                    else f"{self.base}/users/{upn}/drive/root/children"
                )
                payload = {
                    "name": seg,
                    "folder": {},
                    "@microsoft.graph.conflictBehavior": "fail",
                }
                resp = self.session.post(post_url, json=payload, timeout=30)
                self._raise_for_status(resp)
                existing = resp.json()
            parent_path = current_path
            parent_item = existing

        return parent_item  # driveItem for last folder

    # ------------- NEW: upload (small and large) -------------

    def _simple_upload_bytes(
        self,
        upn: str,
        dest_path_with_filename: str,
        content: bytes,
        *,
        conflict_behavior: str = "replace",
    ) -> Dict:
        """
        PUT /users/{upn}/drive/root:/{dest_path_with_filename}:/content?@microsoft.graph.conflictBehavior={...}
        Returns resulting driveItem.
        """
        dest = dest_path_with_filename.strip("/")
        url = (
            f"{self.base}/users/{upn}/drive/root:/{quote(dest, safe='/')}:/content"
            f"?@microsoft.graph.conflictBehavior={quote(conflict_behavior)}"
        )
        resp = self.session.put(url, data=content, timeout=300)
        self._raise_for_status(resp)
        return resp.json()

    def _resumable_upload_file(
        self,
        upn: str,
        parent_folder_path: str,
        file_name: str,
        local_file: Path,
        *,
        conflict_behavior: str = "replace",
        chunk_size: int = 5 * 1024 * 1024,  # 5 MB
    ) -> Dict:
        """
        POST /users/{upn}/drive/root:/{parent}/{file_name}:/createUploadSession
        Then PUT chunks to uploadUrl with Content-Range.
        Returns resulting driveItem.
        """
        parent_folder_path = parent_folder_path.strip("/")
        item_path = f"{parent_folder_path}/{file_name}" if parent_folder_path else file_name
        create_url = f"{self.base}/users/{upn}/drive/root:/{quote(item_path, safe='/')}:/createUploadSession"
        payload = {"item": {"@microsoft.graph.conflictBehavior": conflict_behavior, "name": file_name}}
        resp = self.session.post(create_url, json=payload, timeout=60)
        self._raise_for_status(resp)
        session = resp.json()
        upload_url = session.get("uploadUrl")
        if not upload_url:
            raise RuntimeError("Failed to create upload session (missing uploadUrl)")

        total = local_file.stat().st_size
        with local_file.open("rb") as fh:
            start = 0
            while True:
                chunk = fh.read(chunk_size)
                if not chunk:
                    break
                end = start + len(chunk)
                headers = {
                    "Content-Length": str(len(chunk)),
                    "Content-Range": f"bytes {start}-{end - 1}/{total}",
                }
                put = self.session.put(upload_url, headers=headers, data=chunk, timeout=600)
                # 202 means "continue"; 200/201 returns the final driveItem
                if put.status_code in (200, 201):
                    return put.json()
                if put.status_code not in (200, 201, 202):
                    self._raise_for_status(put)
                # For 202, Graph returns nextExpectedRanges; we advance sequentially
                start = end

        # If loop ended without 200/201, try to GET the item by path
        # (rare, but defensive)
        item = self.get_item_by_path(upn, item_path)
        if item is None:
            raise RuntimeError("Upload session completed but item not found by path.")
        return item

    def upload_local_file_into_same_named_folder(
        self,
        upn: str,
        local_relative_path: str,
        *,
        base_folder_path: Optional[str] = None,
        folder_name_mode: str = "file-stem",  # "file-name" | "file-stem"
        conflict_behavior: str = "replace",   # "replace" | "rename" | "fail"
        large_file_threshold: int = 4 * 1024 * 1024,
        chunk_size: int = 5 * 1024 * 1024,
    ) -> Dict:
        """
        Upload a *local* file into OneDrive under a folder whose name matches the file.
        The folder is created if it does not exist.

        - Folder name rules (default is file-stem):
            - folder_name_mode="file-stem" -> folder name equals the stem (e.g., "Week 46")
            - folder_name_mode="file-name" -> folder name equals the full file name (e.g., "Week 46.xlsx")

        - Destination path (relative to root) becomes:
            {base_folder_path}/{folder_name}/{file_name}
          with segments omitted if not provided.

        Returns the resulting driveItem JSON of the uploaded file.

        Endpoints used:
          - GET  /users/{upn}/drive/root:/{path}
          - POST /users/{upn}/drive/root:/{parent}:/children    (create folder segments)
          - PUT  /users/{upn}/drive/root:/{path}/{file}:/content (small upload)
          - POST /users/{upn}/drive/root:/{path}/{file}:/createUploadSession (large upload)
        """
        # Resolve and read the local file
        local_path = Path(local_relative_path)
        if not local_path.is_file():
            raise FileNotFoundError(f"Local path does not exist or is not a file: {local_relative_path}")

        file_name = local_path.name
        file_stem = local_path.stem

        # Use stem unless explicitly forced to file-name
        _mode = (folder_name_mode or "file-stem").strip().lower()
        folder_name = file_stem if _mode != "file-name" else file_name

        # Safety: if the chosen folder name contains a dot, fall back to the stem
        # (prevents creating folders like "... Initial.xlsx")
        if "." in folder_name:
            folder_name = file_stem

        # Compose destination folder path under root
        parts = []
        if base_folder_path:
            parts.append(base_folder_path.strip("/"))
        parts.append(folder_name)
        dest_folder_path = "/".join(p for p in parts if p)

        # Ensure destination folder exists
        self.ensure_folder_path(upn, dest_folder_path)

        # Choose upload strategy
        size = local_path.stat().st_size
        if size <= large_file_threshold:
            # small (simple) upload
            dest = f"{dest_folder_path}/{file_name}" if dest_folder_path else file_name
            with local_path.open("rb") as fh:
                content = fh.read()
            return self._simple_upload_bytes(
                upn,
                dest,
                content,
                conflict_behavior=conflict_behavior,
            )
        else:
            # large (resumable) upload
            return self._resumable_upload_file(
                upn=upn,
                parent_folder_path=dest_folder_path,
                file_name=file_name,
                local_file=local_path,
                conflict_behavior=conflict_behavior,
                chunk_size=chunk_size,
            )

    @staticmethod
    def _raise_for_status(resp: requests.Response):
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            try:
                detail = json.dumps(resp.json(), indent=2)
            except Exception:
                detail = resp.text
            raise requests.HTTPError(f"{e}\nResponse detail: {detail}") from None
