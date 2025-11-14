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

def clean_sheetname_column_to_dates(obj):
    """
    Convert 'SheetName' column to date type
    """
    # Keep only numbers, periods, hyphens, and slashes


    # Dynamically determine the year and add it to the date range
    # - The data is always from within 3 weeks of the current date
    # - So for example, if it's 11/10/2025, the data could be from the week starting 10/19/2025 through the week ending 12/6
    # - This context warrants support for edge cases towards the end of December and the beginning of January for year changes

