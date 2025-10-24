#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prints the file structure with folder/file names for the entire Ad-Lids folder.

OneDrive (Paul Wolk) → List folders/files → Download Excel → Aggregate best vendor price per product → Upload Summary.xlsx

Auth: Microsoft Entra ID, Microsoft Graph (Application permissions)
Required (admin-consented) app roles for app-only flow:
  - Files.ReadWrite.All
Optional:
  - User.Read.All (only if you need to look up users)
For large uploads via upload sessions:
  - Sites.ReadWrite.All

Environment:
  - Python 3.10+
  - pip install: msal requests pandas openpyxl

Security:
  - Provide CLIENT_SECRET via environment variable GRAPH_CLIENT_SECRET (recommended)
"""

from __future__ import annotations

import os
import io
import sys
import json
import base64
import time
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests
from urllib.parse import quote

# Optional analytics/aggregation
try:
    import pandas as pd
except ImportError:
    pd = None  # We'll check later and give a friendly message.


# -----------------------
# Configuration
# -----------------------

@dataclass
class Config:
    TENANT_ID: str = "549e5d19-024b-4b10-ac2e-58d96a47fe41"
    CLIENT_ID: str = "eac8976a-19da-4bf7-ba45-0b483ad23e4f"
    CLIENT_SECRET: str = os.getenv("GRAPH_CLIENT_SECRET", "")  # set in env; do NOT hardcode secrets in code
    SCOPE: List[str] = None
    GRAPH_BASE: str = "https://graph.microsoft.com/v1.0"

    # Target OneDrive (Paul)
    OWNER_UPN: str = "pwolk@russdaviswholesale.com"
    BASE_FOLDER_PATH: str = "Ad Lids"   # path under OneDrive root

    # Aggregation options (provide your actual column names!)
    # These are placeholders—update to match your files.
    PRODUCT_COL: Optional[str] = None  # e.g., "Product"
    VENDOR_COL: Optional[str] = None   # e.g., "Vendor"
    PRICE_COL: Optional[str] = None    # e.g., "Price"
    SHEET_NAME: Optional[str] = None   # None = first sheet; or a string/sheet index

    # Output file name to write back into BASE_FOLDER_PATH
    OUTPUT_FILENAME: str = "Summary.xlsx"

    # Recursion toggle
    RECURSIVE: bool = True

    # Upload: if True and file <= 250 MB, use PUT /content; else use upload session
    USE_UPLOAD_SESSION_FOR_LARGE: bool = True
    CHUNK_SIZE: int = 5 * 1024 * 1024  # 5 MB per chunk for upload session

    def __post_init__(self):
        if self.SCOPE is None:
            self.SCOPE = ["https://graph.microsoft.com/.default"]


# -----------------------
# Logging
# -----------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("onedrive-aggregate")


# -----------------------
# Auth (MSAL)
# -----------------------

def acquire_app_token(cfg: Config) -> str:
    """Acquire app-only token using client credentials flow."""
    if not cfg.CLIENT_SECRET:
        raise RuntimeError("CLIENT_SECRET is empty. Set env var GRAPH_CLIENT_SECRET with your app's client secret.")

    import msal  # local import to keep top clean

    app = msal.ConfidentialClientApplication(
        cfg.CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{cfg.TENANT_ID}",
        client_credential=cfg.CLIENT_SECRET,
    )
    result = app.acquire_token_for_client(scopes=cfg.SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"Auth failed: {result}")
    token = result["access_token"]
    # Optional debug: display roles claim
    try:
        hdr_b64, payload_b64, _ = token.split(".")
        # safe base64url decode with padding
        payload = json.loads(base64.urlsafe_b64decode(payload_b64 + "=" * (-len(payload_b64) % 4)).decode())
        roles = payload.get("roles")
        logger.info("Token roles: %s", roles)
    except Exception:  # best-effort only
        pass
    return token


# -----------------------
# Microsoft Graph helpers
# -----------------------

class GraphClient:
    def __init__(self, access_token: str, base: str = "https://graph.microsoft.com/v1.0"):
        self.base = base.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {access_token}"})

    # ---- listing by UPN+path -----------------------------------------------

    def list_children_by_upn_path(self, upn: str, path: str) -> List[Dict]:
        """List items in a folder via path under the user's OneDrive root."""
        url = f"{self.base}/users/{upn}/drive/root:/{quote(path, safe='/')}:/children"
        items = []
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

    def get_item_by_upn_path(self, upn: str, path: str) -> Dict:
        """Get driveItem metadata by path."""
        url = f"{self.base}/users/{upn}/drive/root:/{quote(path, safe='/')}"
        resp = self.session.get(url, timeout=30)
        self._raise_for_status(resp)
        return resp.json()

    def iter_tree_by_upn_path(self, upn: str, path: str) -> List[Dict]:
        """Recursively gather all items under a path."""
        out = []

        def _walk(p: str):
            children = self.list_children_by_upn_path(upn, p)
            for it in children:
                out.append(it)
                if "folder" in it:
                    _walk(f"{p.rstrip('/')}/{it['name']}")
        _walk(path)
        return out

    # ---- download / upload --------------------------------------------------

    def download_item_content(self, drive_id: str, item_id: str) -> bytes:
        """
        Download a file's primary stream.
        GET /drives/{drive-id}/items/{item-id}/content
        """
        url = f"{self.base}/drives/{drive_id}/items/{item_id}/content"
        resp = self.session.get(url, timeout=180)
        self._raise_for_status(resp)
        return resp.content  # requests follows 302 redirect to pre-auth download URL

    def upload_small_to_path(self, upn: str, folder_path: str, filename: str, data: bytes) -> Dict:
        """
        Upload (≤ 250 MB) via PUT /root:/{folder}/{filename}:/content
        """
        url = f"{self.base}/users/{upn}/drive/root:/{quote(folder_path.rstrip('/') + '/' + filename, safe='/')}:/content"
        resp = self.session.put(url, data=data, timeout=300)
        self._raise_for_status(resp)
        return resp.json()

    def create_upload_session(self, upn: str, folder_path: str, filename: str) -> str:
        """
        Create an upload session for large files.
        POST /drive/root:/{folder}/{filename}:/createUploadSession
        Returns uploadUrl (pre-auth) to which we PUT chunk ranges.
        """
        url = f"{self.base}/users/{upn}/drive/root:/{quote(folder_path.rstrip('/') + '/' + filename, safe='/')}:/createUploadSession"
        body = {
            "item": {
                "@microsoft.graph.conflictBehavior": "replace",
                "name": filename
            }
        }
        resp = self.session.post(url, json=body, timeout=30)
        self._raise_for_status(resp)
        return resp.json()["uploadUrl"]

    def upload_large_via_session(self, upload_url: str, data: bytes, chunk_size: int = 5 * 1024 * 1024) -> None:
        """
        Upload bytes to a previously created upload session in chunks.
        """
        size = len(data)
        start = 0
        while start < size:
            end = min(start + chunk_size, size) - 1
            chunk = data[start:end + 1]
            headers = {
                "Content-Length": str(len(chunk)),
                "Content-Range": f"bytes {start}-{end}/{size}",
            }
            resp = self.session.put(upload_url, headers=headers, data=chunk, timeout=300)
            # 202 (in-progress) or 201/200 (complete) are expected
            if resp.status_code not in (200, 201, 202):
                self._raise_for_status(resp)
            start = end + 1

    # ---- misc ---------------------------------------------------------------

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
# -----------------------
# Discovery / Printing
# -----------------------

def print_folder_tree(gc: GraphClient, upn: str, root_path: str) -> None:
    """Pretty-print a tree for visual inspection."""
    def _print(path: str, indent: int = 0):
        try:
            items = gc.list_children_by_upn_path(upn, path)
        except requests.HTTPError as ex:
            logger.error("Failed to list '%s': %s", path, ex)
            return
        prefix = "  " * indent
        for it in items:
            mark = "[D]" if "folder" in it else "[F]"
            print(f"{prefix}{mark} {it['name']}")
            if "folder" in it:
                _print(f"{path.rstrip('/')}/{it['name']}", indent + 1)
    print(f"Tree for: {root_path}")
    _print(root_path, 0)


def is_excel_name(name: str) -> bool:
    n = name.lower()
    return n.endswith(".xlsx") or n.endswith(".xlsb") or n.endswith(".xls")


def gather_excel_items(gc: GraphClient, upn: str, base_path: str, recursive: bool = True) -> List[Dict]:
    """Return file items for Excel files under base_path."""
    if recursive:
        items = gc.iter_tree_by_upn_path(upn, base_path)
    else:
        items = gc.list_children_by_upn_path(upn, base_path)
    excel_files = [it for it in items if "file" in it and is_excel_name(it.get("name", ""))]
    return excel_files


# -----------------------
# Excel ingestion & aggregation
# -----------------------

def read_excel_bytes_to_df(file_bytes: bytes, sheet_name=None) -> pd.DataFrame:
    """Read an Excel file (bytes) into a DataFrame. Requires pandas + openpyxl for .xlsx."""
    if pd is None:
        raise RuntimeError("pandas is not installed. Run: pip install pandas openpyxl")
    import io
    return pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, engine="openpyxl")


def aggregate_best_price(
    df: pd.DataFrame,
    product_col: str,
    vendor_col: str,
    price_col: str
) -> pd.DataFrame:
    """
    Return, for each product, the row with the lowest price (keeping the vendor).
    Assumes price_col numeric; convert if needed.
    """
    if product_col not in df.columns or vendor_col not in df.columns or price_col not in df.columns:
        missing = [c for c in (product_col, vendor_col, price_col) if c not in df.columns]
        raise ValueError(f"Missing required columns in DataFrame: {missing}. Available: {list(df.columns)}")
    # ensure numeric
    df = df.copy()
    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    df = df.dropna(subset=[price_col])
    # idxmin by product
    idx = df.groupby(product_col)[price_col].idxmin()
    best = df.loc[idx].sort_values(by=[product_col]).reset_index(drop=True)
    return best


def build_summary_workbook_bytes(
    combined_df: Optional[pd.DataFrame],
    best_df: Optional[pd.DataFrame],
    info_sheet: Optional[Dict[str, str]] = None
) -> bytes:
    """
    Write one or more DataFrames to an in-memory .xlsx and return bytes.
    Sheets:
      - "BestPrices" (if provided)
      - "Combined" (if provided)
      - "Info" (key/value table, optional)
    """
    if pd is None:
        raise RuntimeError("pandas is not installed. Run: pip install pandas openpyxl")
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if best_df is not None:
            best_df.to_excel(writer, sheet_name="BestPrices", index=False)
        if combined_df is not None:
            combined_df.to_excel(writer, sheet_name="Combined", index=False)
        if info_sheet:
            info_df = pd.DataFrame({"Key": list(info_sheet.keys()), "Value": list(info_sheet.values())})
            info_df.to_excel(writer, sheet_name="Info", index=False)
    output.seek(0)
    return output.getvalue()


# -----------------------
# Main flow
# -----------------------

def main():
    cfg = Config()

    # Acquire token
    token = acquire_app_token(cfg)
    gc = GraphClient(token, base=cfg.GRAPH_BASE)

    # Optional: print a tree so you can see the structure
    print_folder_tree(gc, cfg.OWNER_UPN, cfg.BASE_FOLDER_PATH)

    # Gather Excel files (recursive or not)
    excel_items = gather_excel_items(gc, cfg.OWNER_UPN, cfg.BASE_FOLDER_PATH, recursive=cfg.RECURSIVE)
    if not excel_items:
        logger.warning("No Excel files found under '%s'", cfg.BASE_FOLDER_PATH)
    else:
        logger.info("Found %d Excel files.", len(excel_items))

    # Download and load to DataFrames
    combined_rows = []
    if pd is None and excel_items:
        logger.error("pandas not installed; cannot read Excel files. Install: pip install pandas openpyxl")
        sys.exit(2)

    for it in excel_items:
        name = it.get("name")
        drive_id = it.get("parentReference", {}).get("driveId")
        item_id = it.get("id")
        try:
            file_bytes = gc.download_item_content(drive_id, item_id)
            df = read_excel_bytes_to_df(file_bytes, sheet_name=cfg.SHEET_NAME)
            # If sheet_name=None and the workbook contains multiple sheets, pandas returns a dict; handle both cases.
            if isinstance(df, dict):
                # take first sheet by insertion order
                df = next(iter(df.values()))
            df["__SourceFile"] = name
            combined_rows.append(df)
            logger.info("Loaded: %s (%d rows, %d cols)", name, df.shape[0], df.shape[1])
        except Exception as ex:
            logger.error("Failed to process '%s': %s", name, ex)

    combined_df = pd.concat(combined_rows, ignore_index=True) if combined_rows else None

    # Aggregate: best vendor price per product (requires you to set column names in Config)
    best_df = None
    if combined_df is not None:
        if cfg.PRODUCT_COL and cfg.VENDOR_COL and cfg.PRICE_COL:
            try:
                best_df = aggregate_best_price(combined_df, cfg.PRODUCT_COL, cfg.VENDOR_COL, cfg.PRICE_COL)
                logger.info("Aggregated best prices: %d products", best_df.shape[0])
            except Exception as ex:
                logger.error("Aggregation failed: %s", ex)
        else:
            logger.warning(
                "Aggregation columns not set. Update Config.PRODUCT_COL/VENDOR_COL/PRICE_COL to compute BestPrices."
            )

    # Build Summary.xlsx in memory (includes Combined and BestPrices if available)
    if combined_df is None and best_df is None:
        logger.warning("Nothing to write—no data loaded.")
        return

    info = {
        "GeneratedAt": time.strftime("%Y-%m-%d %H:%M:%S"),
        "SourceFolder": cfg.BASE_FOLDER_PATH,
        "Recursive": str(cfg.RECURSIVE),
    }
    summary_bytes = build_summary_workbook_bytes(combined_df, best_df, info_sheet=info)

    # Upload Summary.xlsx back into the same folder
    try:
        # If <= 250 MB use small upload; else upload session
        if len(summary_bytes) <= 250 * 1024 * 1024:
            up_resp = gc.upload_small_to_path(cfg.OWNER_UPN, cfg.BASE_FOLDER_PATH, cfg.OUTPUT_FILENAME, summary_bytes)
            web_url = up_resp.get("webUrl", "<no webUrl>")
            logger.info("Uploaded (small PUT): %s", web_url)
        else:
            if not cfg.USE_UPLOAD_SESSION_FOR_LARGE:
                raise RuntimeError("Output > 250MB and USE_UPLOAD_SESSION_FOR_LARGE is False.")
            upload_url = gc.create_upload_session(cfg.OWNER_UPN, cfg.BASE_FOLDER_PATH, cfg.OUTPUT_FILENAME)
            gc.upload_large_via_session(upload_url, summary_bytes, cfg.CHUNK_SIZE)
            logger.info("Uploaded via upload session.")
    except requests.HTTPError as ex:
        logger.error("Upload failed: %s", ex)
        raise

    logger.info("Done.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
