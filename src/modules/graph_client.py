# graph_client.py
from __future__ import annotations

import json
from typing import Dict, List

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

