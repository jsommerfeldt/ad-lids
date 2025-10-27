# query.py
from __future__ import annotations
import pandas as pd

# OneDriveFolderQuery
from typing import Dict, List, Optional
from modules.graph_client import GraphClient
from modules.utils import file_extension

# SQLServerQuery
#from typing import Optional
import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


class OneDriveFolderQuery:
    """
    Query a OneDrive folder tree and return a tabular view suitable for filtering.

    Parameters
    ----------
    graph_client : GraphClient
        Authenticated client (app-only token).
    owner_upn : str
        e.g., "pwolk@russdaviswholesale.com".
    base_root : str
        Starting folder under the user's OneDrive root (e.g., "Ad Lids").

    Methods
    -------
    to_dataframe(recursive=True, include_files=True, include_folders=True, name_contains=None)
        Returns DataFrame with columns:
        ["Type", "Name", "Path", "File Type", "DriveItemId", "WebUrl", "Size", "LastModified"]
    """

    def __init__(self, graph_client: GraphClient, owner_upn: str, base_root: str):
        self.gc = graph_client
        self.upn = owner_upn
        self.base_root = base_root.strip("/")

    # ---------- helpers ----------
    def bytes_to_human(self, num_bytes):
        if num_bytes is None:
            return ""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if num_bytes < 1024:
                return f"{num_bytes:.1f} {unit}"
            num_bytes /= 1024
        return f"{num_bytes:.1f} PB"

    # ---------- internal traversal ----------
    def _walk(self, path: str) -> List[Dict]:
        """
        Recursively walk the tree under `path` (relative to OneDrive root)
        and collect raw DriveItem JSONs.
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

    # ---------- path normalization ----------
    def _canonical_parent_to_relative(self, canonical_parent_path: str) -> str:
        """
        Convert Graph's canonical parent path to a directory path relative to `self.base_root`.
        Examples of canonical_parent_path:
          "/drive/root:/Ad Lids/Week 41"
          "/drives/{id}/root:/Ad Lids/Week 41"
        We:
          1) Trim the prefix up to ".../root:"
          2) Strip leading slash
          3) Remove the base_root prefix if present
        Returns: "", "Week 41", "Week 41/Subfolder", etc.
        """
        if not canonical_parent_path:
            return ""

        # 1) after '/root:'
        after_root = (
            canonical_parent_path.split("/root:", 1)[1]
            if "/root:" in canonical_parent_path
            else canonical_parent_path
        )

        # 2) drop leading slash
        after_root = after_root.lstrip("/")

        # 3) strip base_root
        base = self.base_root
        if not after_root:
            return ""
        if after_root == base:
            return ""
        if after_root.startswith(base + "/"):
            return after_root[len(base) + 1 :]
        return after_root  # fallback (should be rare if we start at base_root)

    # ---------- public API ----------
    def to_dataframe(
        self,
        recursive: bool = True,
        include_files: bool = True,
        include_folders: bool = True,
        name_contains: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Build a DataFrame of items under the base root.

        Columns
        -------
        Type         : "FOLDER" | "FILE"
        Name         : item['name']
        Path         : directory path relative to base_root (no trailing slash)
        File Type    : extension (e.g., "xlsx") for files, else None
        DriveItemId  : item['id']
        WebUrl       : item['webUrl']
        Size         : item['size'] (files)
        LastModified : item['lastModifiedDateTime']
        """
        # load raw items
        items = self._walk(self.base_root) if recursive else \
                self.gc.list_children_by_upn_path(self.upn, self.base_root)

        needle = (name_contains or "").lower()
        records: List[Dict] = []

        for it in items:
            name = it.get("name", "")
            is_folder = "folder" in it
            is_file = "file" in it

            # type filters
            if is_folder and not include_folders:
                continue
            if is_file and not include_files:
                continue

            # substring filter
            if needle and needle not in name.lower():
                continue
            parent_path = (it.get("parentReference") or {}).get("path", "")
            rel_dir = self._canonical_parent_to_relative(parent_path)

            size_bytes = it.get("size") if "file" in it else None
            rec = {
                "Type": "FOLDER" if is_folder else "FILE",
                "Name": name,
                "Path": rel_dir,
                "File Type": None if is_folder else file_extension(name),
                "DriveItemId": it.get("id"),
                "WebUrl": it.get("webUrl"),
                "Size": self.bytes_to_human(size_bytes),
                "LastModified": it.get("lastModifiedDateTime"),
            }
            records.append(rec)

        df = pd.DataFrame.from_records(
            records,
            columns=[
                "Type", "Name", "Path", "File Type",
                "DriveItemId", "WebUrl", "Size", "LastModified",
            ],
        )

        # optional ordering: folders first, then by Path/Name (stable mergesort)
        if not df.empty:
            df["__sort_type"] = df["Type"].map({"FOLDER": 0, "FILE": 1})
            df.sort_values(["__sort_type", "Path", "Name"], inplace=True, kind="mergesort")
            df.drop(columns="__sort_type", inplace=True)

        return df

    def order_by_top_folder_block(self, df: pd.DataFrame, root_position: str = "top") -> pd.DataFrame:
        """
        Reorder `df` so each TOP-LEVEL folder row is followed by all its descendants.

        Rules:
        - Top-level folders (Type=='FOLDER' & Path=='') sorted by Name (case-insensitive).
        - Within each folder block:
            Anchor row first -> shallower Path depth before deeper -> folders before files -> Name.
        - Root-level files (Type=='FILE' & Path=='') grouped at 'top' or 'bottom' via root_position.

        Parameters
        ----------
        df : DataFrame from to_dataframe()
        root_position : {"top","bottom"}, default "top"
            Where to place root files not inside any folder.

        Returns
        -------
        pd.DataFrame (re-ordered copy)
        """
        required = {"Type", "Name", "Path"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Missing required column(s): {sorted(missing)}")

        dfx = df.copy()

        # depth of Path: "" -> 0; "A/B" -> 2
        def _depth(p: str) -> int:
            if not isinstance(p, str) or p == "":
                return 0
            return len(p.split("/"))

        dfx["__depth"] = dfx["Path"].fillna("").map(_depth)

        # anchors = top-level folders
        is_anchor = (dfx["Type"] == "FOLDER") & (dfx["Path"].fillna("") == "")
        dfx["__is_anchor"] = is_anchor.astype(int)

        # assign an anchor name to each row
        def _anchor(row):
            path = row["Path"] or ""
            if row["Type"] == "FOLDER" and path == "":
                return row["Name"]
            if path:
                return path.split("/")[0]
            return None  # root file

        dfx["__anchor"] = dfx.apply(_anchor, axis=1)

        # global order of anchors (alphabetical by name)
        anchors_sorted = (
            dfx.loc[is_anchor, "Name"]
               .astype(str)
               .sort_values(key=lambda s: s.str.lower())
               .unique()
               .tolist()
        )
        anchor_rank = {name: i for i, name in enumerate(anchors_sorted)}

        # root files block position
        root_group_order = -1 if root_position == "top" else len(anchors_sorted)
        dfx["__anchor_order"] = dfx["__anchor"].map(anchor_rank).fillna(root_group_order)

        # ensure the anchor row itself comes before its children
        dfx["__is_anchor_order"] = (1 - dfx["__is_anchor"])  # 0 for anchor, 1 for others

        # folders before files within same level
        dfx["__type_order"] = dfx["Type"].map({"FOLDER": 0, "FILE": 1}).fillna(2)

        dfx = dfx.sort_values(
            by=["__anchor_order", "__is_anchor_order", "__depth", "Path", "__type_order", "Name"],
            kind="mergesort",
        )

        return dfx.drop(
            columns=["__depth", "__is_anchor", "__anchor", "__anchor_order", "__is_anchor_order", "__type_order"]
        ).reset_index(drop=True)

class SQLServerQuery:

    def __init__(self, server: str = "HA-PWRBISQL23", database: str = "master", driver: str = "ODBC Driver 17 for SQL Server"):
        self.server: str = server
        self.database: str = database
        self.driver: str = driver
        self.trusted_connection: str = "yes"

    # --- Connection Helpers -------------------------------------------------------
    def get_engine(self) -> Engine:
        """
        Create a SQLAlchemy engine for SQL Server using Windows Authentication.
        Mirrors the connection style you already use.

        Parameters
        ----------
        server : str
            SQL Server hostname or instance.
        database : str
            Database to use as the initial context (use 'master' for linked server queries).
        driver : str
            ODBC driver name installed on the host machine.
        trusted_connection : str
            'yes' for Windows Auth; typically leave as 'yes' inside your network.

        Returns
        -------
        Engine
            A SQLAlchemy engine suitable for pandas.read_sql_query and SQL execution.
        """
        conn_str = (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"Trusted_Connection={self.trusted_connection};"
        )
        params = urllib.parse.quote_plus(conn_str)
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)
        return engine

    # --- Public API ---------------------------------------------------------------
    def fetch_data(self, query, engine: Optional[Engine] = None) -> pd.DataFrame:
        """
        Execute the provided OPENQUERY against the linked server PPRODW and return results.

        Parameters
        ----------
        engine : Optional[Engine]
            Optionally pass an existing SQLAlchemy Engine. If None, a temporary engine
            is created using get_engine() and disposed after the call.

        Returns
        -------
        pandas.DataFrame
            The result set as a DataFrame.
        """
        close_engine = False
        if engine is None:
            engine = self.get_engine()
            close_engine = True
        try:
            # Use a connection context to ensure timely release back to the pool
            with engine.connect() as conn:
                df = pd.read_sql_query(query, conn)
            return df
        finally:
            if close_engine:
                engine.dispose()

