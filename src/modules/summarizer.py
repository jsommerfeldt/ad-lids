# summarizer.py
from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple, Mapping

import numpy as np
import pandas as pd
import re

from modules.graph_client import GraphClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def compute_target_week_folders(sundayweeknumber: int, horizon: int = 3) -> List[str]:
    """
    For sundayweeknumber=W, return:
      ["Week W Final Week W+1 Initial", "Week W+1 Final Week W+2 Initial", "Week W+2 Final Week W+3 Initial"]
    """
    return [f"Week {w} Final Week {w+1} Initial" for w in range(sundayweeknumber, sundayweeknumber + horizon)]


@dataclass(frozen=True)
class FolderInfo:
    name: str
    drive_item_id: Optional[str]  # from inventory (FOLDER row)
    found_in_inventory: bool


class WeekFolderResolver:
    """
    Uses the inventory DataFrame (from your exported CSV) to resolve the 3 folders for a given week.
    Inventory is expected to have columns: ['Type','Name','Path','File Type','DriveItemId','WebUrl','Size','LastModified']
    """

    def __init__(self, inventory_df: pd.DataFrame):
        self.df = inventory_df.copy()

    def resolve_relevant_folders(self, sundayweeknumber: int, horizon: int = 3) -> List[FolderInfo]:
        targets = compute_target_week_folders(sundayweeknumber, horizon=horizon)

        # Limit to top-level folders: Type=='FOLDER' and Path=='' (or NaN)
        dff = self.df.copy()
        dff["Path"] = dff["Path"].fillna("")
        top_folders = dff[(dff["Type"] == "FOLDER") & (dff["Path"] == "")]

        results: List[FolderInfo] = []
        for t in targets:
            row = top_folders.loc[top_folders["Name"] == t]
            if not row.empty:
                results.append(
                    FolderInfo(
                        name=t,
                        drive_item_id=row.iloc[0]["DriveItemId"],
                        found_in_inventory=True,
                    )
                )
            else:
                results.append(FolderInfo(name=t, drive_item_id=None, found_in_inventory=False))
        return results

    def files_under_folder_name(self, folder_name: str) -> pd.DataFrame:
        """
        Return rows for all FILE items whose Path equals the folder_name or starts with 'folder_name/'.
        Includes files in nested subfolders.
        """
        dff = self.df.copy()
        # We only need files
        dff = dff[dff["Type"] == "FILE"]
        dff["Path"] = dff["Path"].astype(str)
        mask = (dff["Path"] == folder_name) | (dff["Path"].str.startswith(folder_name + "/"))
        return dff.loc[mask].reset_index(drop=True)


class FolderSummarizer:
    """
    Summarize *one* folder's files by producing one DataFrame per file.

    - Downloads file bytes via Graph.
    - Parses Excel files (.xlsx) into DataFrames (by default: first sheet).
    - Skips files of unknown/unsupported type (you can register custom handlers).
    """

    def __init__(
        self,
        graph_client: GraphClient,
        owner_upn: str,
        file_parser: Optional[Callable[[bytes, str], pd.DataFrame]] = None,
    ):
        self.gc = graph_client
        self.owner_upn = owner_upn
        self.file_parser = file_parser or self._excel_all_sheets_concat

    # ---------- public API ----------

    def summarize_folder_by_name(self, inventory_df: pd.DataFrame, folder_name: str) -> Dict[str, pd.DataFrame]:
        """
        Returns: { file_name: DataFrame } for the given folder.
        If a file cannot be parsed into a DataFrame, it is omitted from the dict (and logged).
        """
        resolver = WeekFolderResolver(inventory_df)
        file_rows = resolver.files_under_folder_name(folder_name)

        outputs: Dict[str, pd.DataFrame] = {}
        for _, row in file_rows.iterrows():
            name = str(row.get("Name", ""))
            ext = str(row.get("File Type", "")).lower()
            item_id = str(row.get("DriveItemId", ""))

            # Only handle Excel for now.
            if ext not in ("xlsx", "xlsm", "xls"):
                logger.info("Skipping non-Excel file: %s", name)
                continue

            try:
                content = self.gc.download_item_content_by_user_item(self.owner_upn, item_id)
            except Exception as e:
                logger.exception("Download failed for %s (%s): %s", name, item_id, e)
                continue

            try:
                df = self.file_parser(content, name)
            except Exception as e:
                logger.exception("Parsing failed for %s: %s", name, e)
                continue

            outputs[name] = df

        return outputs

    # ---------- default parser (all sheets) ----------
    @staticmethod
    def _excel_all_sheets_concat(content: bytes, file_name: str) -> pd.DataFrame:
        """
        Loads *all* sheets of an Excel file and concatenates them into a single DataFrame.
        Adds 'SourceFile' and 'SheetName' columns for traceability.
        """
        bio = io.BytesIO(content)
        sheets = pd.read_excel(bio, sheet_name=None, engine="openpyxl")

        frames = []
        for sheet_name, df in sheets.items():
            # 1) Skip truly empty DataFrames (0 rows, 0 cols)
            if df is None or (hasattr(df, "empty") and df.empty and df.shape[1] == 0):
                continue

            # 2) Drop columns that are entirely NA to avoid the concat FutureWarning
            base = df.copy()
            base = base.dropna(axis=1, how="all")

            # 3) If after dropping all-NA columns the sheet has no informative content, skip it
            if base.shape[1] == 0 or not base.notna().any().any():
                continue

            # 4) Add provenance columns last (so they don't affect the NA checks)
            base.insert(0, "SheetName", str(sheet_name))
            base.insert(0, "SourceFile", file_name)

            frames.append(base)

        if not frames:
            # Create a minimal marker row so caller doesn’t fail on empty workbook
            return pd.DataFrame([{
                "SourceFile": file_name,
                "SheetName": "(empty workbook)"
            }])

        # Concatenate without the all-NA columns/frames included above
        return pd.concat(frames, ignore_index=True)

class ThreeWeekSummarizer:
    """
    Orchestrates the 3-folder workflow for a given sundayweeknumber.
    """

    def __init__(self, graph_client: GraphClient, owner_upn: str, inventory_df: pd.DataFrame):
        self.gc = graph_client
        self.owner_upn = owner_upn
        self.inventory_df = inventory_df.copy()
        self.resolver = WeekFolderResolver(self.inventory_df)
        self.folder_summarizer = FolderSummarizer(self.gc, self.owner_upn)

    def _parse_dates_explicit(self, series_obj: pd.Series) -> tuple[pd.Series, pd.Series]:
        """
        Return:
        - mask: boolean Series (index-aligned) indicating which entries have
            MORE THAN 7 characters AFTER removing all non-number characters
            (except for periods) from the series.
        - cleaned: string Series (index-aligned) of the normalized string where
            all non-number characters except periods are removed.

        Examples before length check:
        "2025-10-18 00:00:00" -> "20251018000000" (len 14) => date-like
        "$     34.95"         -> "34.95"         (len 5)  => not date-like
        "13.5"                -> "13.5"          (len 4)  => not date-like
        """
        s = series_obj.astype("object")

        # Normalize everything to string; treat NaN/None as empty
        def _to_str(v):
            if v is None:
                return ""
            # Keep numeric representations (e.g., 13.5) as strings
            if isinstance(v, (int, float, np.integer, np.floating)):
                # Convert NaN to empty
                if isinstance(v, float) and np.isnan(v):
                    return ""
                return str(v)
            return str(v)

        as_str = s.apply(_to_str)

        # Remove all non-number characters except periods
        cleaned = as_str.str.replace(r"[^0-9.]", "", regex=True)

        # Length > 7 indicates "date-like" per your heuristic
        mask = cleaned.str.len() > 7

        # Ensure alignment & types
        mask = mask.fillna(False)
        cleaned = cleaned.fillna("")

        # Return BOTH the boolean mask and the cleaned text series
        return mask, cleaned
    
    def _clean_ad_lid_price(self, series: pd.Series) -> pd.Series:
        s = series.copy()

        # Normalize whitespace on strings
        def _strip_if_str(v):
            return v.strip() if isinstance(v, str) else v
        s = s.map(_strip_if_str)

        # If native datetime dtype, they are all dates (treat as missing for price)
        if pd.api.types.is_datetime64_any_dtype(s):
            return pd.Series([np.nan] * len(s), index=s.index)

        s_obj = s.astype("object")

        # Get BOTH: date-like mask and cleaned string values
        is_date_like, cleaned_text = self._parse_dates_explicit(s_obj)

        # Apply: set date-like to NaN, keep cleaned string elsewhere
        out = cleaned_text.mask(is_date_like, other=np.nan)

        # Empty strings to NaN
        out = out.replace("", np.nan)

        # Optionally: convert to float numeric (uncomment if you want numeric result)
        # This will turn valid cleaned prices like "23.22" -> 23.22 and leave NaN as NaN.
        # out = pd.to_numeric(out, errors="coerce")

        return out
    
    def run(self, sundayweeknumber: int, horizon: int = 3) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Returns: { folder_name : { file_name : DataFrame } }
        """
        folder_infos = self.resolver.resolve_relevant_folders(sundayweeknumber, horizon=horizon)
        results: Dict[str, Dict[str, pd.DataFrame]] = {}

        # Optional: prefer reverse chronological (highest W first)
        # Just sort by the numeric first week in the folder name.
        def _first_week_num(fname: str) -> int:
            # "Week 46 Final Week 47 Initial" -> 46
            try:
                return int(fname.split()[1])
            except Exception:
                return -1

        for fi in sorted(folder_infos, key=lambda x: _first_week_num(x.name), reverse=True):
            if not fi.found_in_inventory:
                logger.warning("Folder not found in inventory: %s", fi.name)
                continue

            per_file = self.folder_summarizer.summarize_folder_by_name(self.inventory_df, fi.name)
            results[fi.name] = per_file

        return results



    def summarize_books(
        self,
        results: "Mapping[str, Mapping[str, pd.DataFrame]]",
        *,
        how: str = "union",
        add_folder_col: bool = True,
        add_ad_lid_price_only_sheet: bool = True,   # NEW: toggle the second worksheet
        ad_lid_price_col: str = "Ad Lid Price"      # Column name to inspect/clean
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Consolidate each folder's per-file DataFrames into one consolidated DataFrame,
        and (optionally) add a second DataFrame with only rows having a non-empty, non-date
        Ad Lid Price value after cleaning.

        Returns:
            {
              folder_name: {
                  "Consolidated": DataFrame,
                  "AdLidPriceOnly": DataFrame   # included if add_ad_lid_price_only_sheet
              }
            }
        """
        out: Dict[str, Dict[str, pd.DataFrame]] = {}

        for folder_name, files_map in results.items():
            if not files_map:
                out[folder_name] = {}
                continue
            frames: List[pd.DataFrame] = []
            for file_name, df in files_map.items():
                if df is None:
                    continue
                d = df.copy()

                # Ensure provenance columns exist
                if "SourceFile" not in d.columns:
                    d.insert(0, "SourceFile", file_name)
                if "SheetName" not in d.columns:
                    d.insert(1, "SheetName", "(unknown)")
                if add_folder_col and "Folder" not in d.columns:
                    d.insert(0, "Folder", folder_name)

                # Drop all-NA columns (prevents concat FutureWarning and keeps things tidy)
                d = d.dropna(axis=1, how="all")

                frames.append(d)

            if not frames:
                out[folder_name] = {}
                continue

            # Combine frames
            if how == "intersection":
                common = set(frames[0].columns)
                for f in frames[1:]:
                    common &= set(f.columns)
                cols = [c for c in frames[0].columns if c in common]
                frames = [f[cols] for f in frames]
                combined = pd.concat(frames, ignore_index=True)
            else:
                combined = pd.concat(frames, ignore_index=True)

            # Always keep a consolidated sheet
            out[folder_name] = {"Consolidated": combined}

            # Optionally add AdLidPriceOnly sheet
            if add_ad_lid_price_only_sheet:
                if ad_lid_price_col in combined.columns:
                    cleaned = self._clean_ad_lid_price(combined[ad_lid_price_col])
                    mask = cleaned.notna()
                    # Choose the same columns as consolidated; add a cleaned helper col for auditing
                    ad_only = combined.loc[mask].copy()
                    ad_only[ad_lid_price_col] = cleaned.loc[mask]

                # ---------------------------- NEW: Phased stable sort for AdLidPriceOnly ----------------------------
                # Phase 1: Sort by numeric Item ASC to make groups deterministic and contiguous
                if "Item" in ad_only.columns:
                    _item_num = pd.to_numeric(
                        ad_only["Item"].astype(str).str.strip().str.replace(r"[^0-9.\-]", "", regex=True),
                        errors="coerce"
                    )
                    ad_only = (
                        ad_only
                        .assign(_item_num=_item_num)
                        .sort_values(by="_item_num", ascending=True, na_position="last", kind="mergesort")
                    )

                    # Phase 2: Stable sort by per-Item row count DESC
                    # (keeps the numeric Item order within ties because mergesort is stable)
                    _item_count = ad_only.groupby("Item")["Item"].transform("size")
                    ad_only = (
                        ad_only
                        .assign(_item_count=_item_count)
                        .sort_values(by="_item_count", ascending=False, kind="mergesort")
                        .drop(columns=["_item_num", "_item_count"])
                        .reset_index(drop=True)
                    )

                    # Phase 2.5: Stable sort by earliest "Loading Start Date" per Item (ascending)
                    # - Parses the date column to datetime (invalid -> NaT).
                    # - Computes per-Item minimum start date.
                    # - Sorts by that minimum; NaT goes last.
                    if "Loading Start Date" in ad_only.columns:
                        print("sorting Loading Start Date")
                        _start_dt = pd.to_datetime(ad_only["Loading Start Date"], errors="coerce")
                        _item_min_start = _start_dt.groupby(ad_only["Item"]).transform("min")
                        ad_only = (
                            ad_only
                            .assign(_item_min_start=_item_min_start,
                                    _min_isna=_item_min_start.isna())
                            .sort_values(by=["_min_isna", "_item_min_start"],
                                         ascending=[True, True],
                                         kind="mergesort")
                            .drop(columns=["_item_min_start", "_min_isna"])
                            .reset_index(drop=True)
                        )
                    
                    # Phase 3 - in-group ascending sort by Ad Lid Price
                    # Preserve current Item order exactly as arranged by phases 1 & 2
                    _item_order_map = {val: i for i, val in enumerate(ad_only["Item"].drop_duplicates())}
                    _price_num = pd.to_numeric(ad_only[ad_lid_price_col], errors="coerce")

                    # Parse row-level start date (not the per-Item min)
                    _start_dt_row = pd.to_datetime(ad_only["Loading Start Date"], errors="coerce") \
                        if "Loading Start Date" in ad_only.columns else pd.Series(pd.NaT, index=ad_only.index)

                    # Sort within each Item (least -> greatest), keeping Item groups in the same order.
                    # NaNs (missing/invalid prices) go to the bottom of each Item group.
                    ad_only = (
                        ad_only
                        .assign(
                            _item_order=ad_only["Item"].map(_item_order_map),
                            _start_dt_row = _start_dt_row,
                            _price_num=_price_num,
                            _price_isna=_price_num.isna()
                        )
                        .sort_values(
                            by=["_item_order", "_start_dt_row", "_price_isna", "_price_num"],
                            ascending=[True, True, True, True],
                            na_position="last",
                            kind="mergesort",             # stable: preserves row order within ties
                        )
                        .drop(columns=["_item_order", "_start_dt_row", "_price_num", "_price_isna"])
                        .reset_index(drop=True)
                    )
                    # ------------------------------------------------------------------------------------------------------

                    # You could also keep the original in a separate column for comparison:
                    # ad_only["Ad Lid Price (raw)"] = combined[ad_lid_price_col].loc[mask]
                    out[folder_name]["AdLidPriceOnly"] = ad_only
                else:
                    # Column missing: create an empty sheet with just provenance for clarity
                    out[folder_name]["AdLidPriceOnly"] = combined.head(0).copy()

        return out
