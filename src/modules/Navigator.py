#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Class for querying OneDrive (Paul Wolk) via Microsoft Graph and returning a tabular view.

- App-only auth (client credentials) using MSAL.
- Path-based traversal under a known base path (e.g., "Ad Lids").
- Returns a pandas DataFrame with columns:
    Type, Name, Path, File Type, DriveItemId, WebUrl, Size, LastModified

Notes
-----
- Set CLIENT_SECRET via environment variable GRAPH_CLIENT_SECRET (recommended).
- You already confirmed BASE_FOLDER_PATH = "Ad Lids" works in your tenant.
- If someone renames/moves the folder later, you can add a share-link fallback
  (ID-based) without changing the DataFrame schema.
"""

from __future__ import annotations

import os
import json
import base64
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests
from urllib.parse import quote

import pandas as pd

# ----------------------------------------------------------------
# Logging
# ----------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("graph-files-explorer")

# ----------------------------------------------------------------
# Config
# ----------------------------------------------------------------
@dataclass
class Config:
    TENANT_ID: str = "549e5d19-024b-4b10-ac2e-58d96a47fe41"
    CLIENT_ID: str = "eac8976a-19da-4bf7-ba45-0b483ad23e4f"
    CLIENT_SECRET: str = os.getenv("GRAPH_CLIENT_SECRET", "")  # recommended to set in env
    SCOPE: List[str] = None
    GRAPH_BASE: str = "https://graph.microsoft.com/v1.0"

    OWNER_UPN: str = "pwolk@russdaviswholesale.com"
    BASE_FOLDER_PATH: str = "Ad Lids"   # confirmed by your diagnostics

    def __post_init__(self):
        if self.SCOPE is None:
            self.SCOPE = ["https://graph.microsoft.com/.default"]


# ----------------------------------------------------------------
# Auth helper (MSAL)
# ----------------------------------------------------------------
def acquire_app_token(cfg: Config) -> str:
    """Acquire app-only token using client credentials flow."""
    if not cfg.CLIENT_SECRET:
        raise RuntimeError("CLIENT_SECRET empty. Set env var GRAPH_CLIENT_SECRET with your app's client secret.")

    import msal
    app = msal.ConfidentialClientApplication(
        cfg.CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{cfg.TENANT_ID}",
        client_credential=cfg.CLIENT_SECRET,
    )
    result = app.acquire_token_for_client(scopes=cfg.SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"Auth failed: {result}")
    token = result["access_token"]

    # Optional: log token roles (app-only permissions)
    try:
        _, payload_b64, _ = token.split(".")
        payload = json.loads(base64.urlsafe_b64decode(payload_b64 + "=" * (-len(payload_b64) % 4)).decode())
        logger.info("Token roles: %s", payload.get("roles"))
    except Exception:
        pass
    return token


# ----------------------------------------------------------------
# Minimal Graph client (reuses your working patterns)
# ----------------------------------------------------------------
class GraphClient:
    def __init__(self, access_token: str, base: str = "https://graph.microsoft.com/v1.0"):
        self.base = base.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {access_token}"})

    def list_children_by_upn_path(self, upn: str, path: str) -> List[Dict]:
        """List items in a folder via path relative to a user's OneDrive root."""
        url = f"{self.base}/users/{upn}/drive/root:/{quote(path, safe='/')}:/children"
        items: List[Dict] = []
        resp = self.session.get(url, timeout=30)
        self._raise_for_status(resp)
        data = resp.json()
        items += data.get("value", [])
        while "@odata.nextLink" in data:
            resp = self.session.get(data["@odata.nextLink"], timeout=30)
            self._raise_for_status(resp)
            data = resp.json()
            items += data.get("value", [])
        return items

    @staticmethod
    def _raise_for_status(resp: requests.Response):
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            detail = ""
            try:
                detail = json.dumps(resp.json(), indent=2)
            except Exception:
                detail = resp.text
            raise requests.HTTPError(f"{e}\nResponse detail: {detail}") from None


# ----------------------------------------------------------------
# Utility: name/extension & path normalization
# ----------------------------------------------------------------
def file_extension(name: str) -> Optional[str]:
    """Return the last extension without dot, or None."""
    if not name or "." not in name:
        return None
    ext = name.rsplit(".", 1)[-1].strip().lower()
    return ext or None


def graph_canonical_to_relative(canonical_parent_path: str, item_name: str, base_root: str) -> str:
    """
    Convert Graph's canonical parent path to a relative path (excluding the item name),
    rooted at base_root (e.g., "Ad Lids").

    Examples of canonical_parent_path forms:
      "/drive/root:/Ad Lids/Week 41" or "/drives/{id}/root:/Ad Lids/Week 41"
    We:
      1) Trim the prefix up to ".../root:"
      2) Normalize leading "/"
      3) Ensure we return the directory path relative to "Ad Lids" (no trailing slash)
    """
    if not canonical_parent_path:
        return ""

    # 1) Strip everything up to "/root:"
    if "/root:" in canonical_parent_path:
        after_root = canonical_parent_path.split("/root:", 1)[1]
    else:
        # Unexpected; fallback to full string as-is
        after_root = canonical_parent_path

    # 2) Ensure a single leading slash, then drop it
    after_root = after_root.lstrip("/")
    # "Ad Lids/Week 41" or maybe "" if parent is exactly root

    # 3) Ensure relative to base_root
    base_root = base_root.strip("/")

    if not after_root:
        # Parent is root; relative path is "" (item is under base_root if base_root=="" else we adjust)
        # If base_root != "" and the item is directly under base_root, canonical parent will be "Ad Lids"
        # so we won't land here.
        return ""

    # If the canonical parent starts with the base, remove it.
    if after_root == base_root:
        return ""  # item lives directly under base_root
    if after_root.startswith(base_root + "/"):
        return after_root[len(base_root) + 1 :]

    # If base_root not present (unexpected if you pointed traversal at base_root), return full
    return after_root


# ----------------------------------------------------------------
# NEW: Class for querying the API and returning a DataFrame
# ----------------------------------------------------------------
class OneDriveFolderQuery:
    """
    Query a OneDrive folder tree and return a tabular view suitable for filtering.

    Parameters
    ----------
    graph_client : GraphClient
        Authenticated client (app-only token).
    owner_upn : str
        The OneDrive (business) owner UPN, e.g., "pwolk@russdaviswholesale.com".
    base_root : str
        The starting folder name under the user's OneDrive root (e.g., "Ad Lids").

    Methods
    -------
    to_dataframe(recursive=True, include_files=True, include_folders=True, name_contains=None)
        Return a pandas DataFrame with at least:
        ["Type", "Name", "Path", "File Type", "DriveItemId", "WebUrl", "Size", "LastModified"].
    """

    def __init__(self, graph_client: GraphClient, owner_upn: str, base_root: str):
        self.gc = graph_client
        self.upn = owner_upn
        self.base_root = base_root.strip("/")

    def _walk(self, path: str) -> List[Dict]:
        """
        Recursively walk the tree under `path` (relative to OneDrive root),
        returning the *raw* Graph items (DriveItem JSONs).
        """
        collected: List[Dict] = []

        def _recurse(current_path: str):
            children = self.gc.list_children_by_upn_path(self.upn, current_path)
            for it in children:
                collected.append(it)
                if "folder" in it:
                    _recurse(f"{current_path.rstrip('/')}/{it['name']}")

        _recurse(path)
        return collected

    def to_dataframe(
        self,
        recursive: bool = True,
        include_files: bool = True,
        include_folders: bool = True,
        name_contains: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Build a DataFrame of items under the base root.

        Columns (at minimum)
        --------------------
        Type        : "FOLDER" | "FILE"
        Name        : item['name']
        Path        : directory path relative to base root (no trailing slash)
        File Type   : file extension (e.g., "xlsx") for files; None for folders
        DriveItemId : item['id']
        WebUrl      : item['webUrl']
        Size        : item['size'] (files only; folders typically omitted)
        LastModified: item['lastModifiedDateTime']
        """
        # Load items (one level or recursive)
        if recursive:
            items = self._walk(self.base_root)
        else:
            items = self.gc.list_children_by_upn_path(self.upn, self.base_root)

        records = []
        needle = (name_contains or "").lower()

        for it in items:
            name = it.get("name", "")
            is_folder = "folder" in it
            is_file = "file" in it

            # Filter type
            if is_folder and not include_folders:
                continue
            if is_file and not include_files:
                continue
            # Filter by name substring (optional)
            if needle and needle not in name.lower():
                continue

            parent_path = (it.get("parentReference") or {}).get("path", "")  # e.g., "/drives/{id}/root:/Ad Lids/Week 41"
            rel_dir = graph_canonical_to_relative(parent_path, name, base_root=self.base_root)

            rec = {
                "Type": "FOLDER" if is_folder else "FILE",
                "Name": name,
                "Path": rel_dir,
                "File Type": None if is_folder else file_extension(name),
                "DriveItemId": it.get("id"),
                "WebUrl": it.get("webUrl"),
                "Size": it.get("size"),
                "LastModified": it.get("lastModifiedDateTime"),
            }
            records.append(rec)

        df = pd.DataFrame.from_records(records, columns=[
            "Type", "Name", "Path", "File Type", "DriveItemId", "WebUrl", "Size", "LastModified"
        ])
        # Optional: make sorting predictable (folders before files, then by path/name)
        if not df.empty:
            df["__sort_type"] = df["Type"].map({"FOLDER": 0, "FILE": 1})
            df.sort_values(["__sort_type", "Path", "Name"], inplace=True, kind="mergesort")
            df.drop(columns="__sort_type", inplace=True)
        return df


# ----------------------------------------------------------------
# Example usage (main)
# ----------------------------------------------------------------
def main():
    cfg = Config()

    # 1) Acquire token and build client
    token = acquire_app_token(cfg)
    gc = GraphClient(token, base=cfg.GRAPH_BASE)

    # 2) Create the query helper rooted at "Ad Lids"
    q = OneDriveFolderQuery(graph_client=gc, owner_upn=cfg.OWNER_UPN, base_root=cfg.BASE_FOLDER_PATH)

    # 3) Get a recursive table of everything under Ad Lids
    df_all = q.to_dataframe(recursive=True, include_files=True, include_folders=True)

    # 5) (Optional) Save the inventory to CSV for inspection
    df_all.to_csv("ad_lids_inventory.csv", index=False)

if __name__ == "__main__":
    main()