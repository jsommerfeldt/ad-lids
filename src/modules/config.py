# config.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Config:
    """
    Centralized configuration for the app.

    NOTE: Provide CLIENT_SECRET via environment variable GRAPH_CLIENT_SECRET.
    """
    TENANT_ID: str = os.getenv("GRAPH_TENANT_ID", "549e5d19-024b-4b10-ac2e-58d96a47fe41")
    CLIENT_ID: str = os.getenv("GRAPH_CLIENT_ID", "eac8976a-19da-4bf7-ba45-0b483ad23e4f")
    CLIENT_SECRET: str = os.getenv("GRAPH_CLIENT_SECRET", "")

    SCOPE: Optional[List[str]] = None
    GRAPH_BASE: str = os.getenv("GRAPH_BASE", "https://graph.microsoft.com/v1.0")

    # OneDrive targeting (your working values)
    OWNER_UPN: str = os.getenv("GRAPH_OWNER_UPN", "pwolk@russdaviswholesale.com")
    BASE_FOLDER_PATH: str = os.getenv("GRAPH_BASE_FOLDER_PATH", "Ad Lids")

    def __post_init__(self):
        if self.SCOPE is None:
            self.SCOPE = ["https://graph.microsoft.com/.default"]
        if not self.CLIENT_SECRET:
            raise RuntimeError(
                "CLIENT_SECRET empty. Set env var GRAPH_CLIENT_SECRET with your app's client secret."
            )
