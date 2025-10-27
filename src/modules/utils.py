# utils.py
from __future__ import annotations
from typing import Optional


def file_extension(name: str) -> Optional[str]:
    """
    Return the last extension (without dot), or None.
    """
    if not name or "." not in name:
        return None
    ext = name.rsplit(".", 1)[-1].strip().lower()
    return ext or None