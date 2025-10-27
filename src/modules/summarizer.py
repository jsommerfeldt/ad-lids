# summarizer.py
from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

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
        self.file_parser = file_parser or self._default_excel_first_sheet

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

    # ---------- default parser (first sheet) ----------

    @staticmethod
    def _default_excel_first_sheet(content: bytes, file_name: str) -> pd.DataFrame:
        """
        Loads the first sheet of an Excel file as a DataFrame.
        Adds a 'SourceFile' column to help with downstream merges.
        """
        bio = io.BytesIO(content)
        # pandas will infer engine openpyxl for .xlsx at runtime in your environment
        # If a file has no sheets or is empty, pandas will raise; caller handles exceptions.
        df_first = pd.read_excel(bio, sheet_name=0, engine="openpyxl")
        df_first.insert(0, "SourceFile", file_name)
        return df_first


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
