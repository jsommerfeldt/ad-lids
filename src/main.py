# main.py
from __future__ import annotations

import logging

from modules.auth import AuthProvider
from modules.config import Config
from modules.graph_client import GraphClient
from modules.query import OneDriveFolderQuery, SQLServerQuery
from modules.summarizer import ThreeWeekSummarizer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")


def main():
    # 1) Load config and acquire token
    cfg = Config()
    token = AuthProvider(cfg).acquire_token()

    # 2) Build Graph client
    gc = GraphClient(token, base=cfg.GRAPH_BASE)

    # 3) Query helper rooted at the working folder ("Ad Lids")
    q = OneDriveFolderQuery(graph_client=gc, owner_upn=cfg.OWNER_UPN, base_root=cfg.BASE_FOLDER_PATH)

    # 4) Get the inventory table (recursive)
    df = q.to_dataframe(recursive=True, include_files=True, include_folders=True)

    # 5) Sort the data
    df = q.order_by_top_folder_block(df=df, root_position="top")

    # 6) Get dim_time table from SQL Server for determining the relevant directory
    ss = SQLServerQuery()
    engine = ss.get_engine()
    time_df = ss.fetch_data(query=f"""\
    SELECT *
    FROM OPENQUERY(PPRODW,
        '
            SELECT sundayweeknumber
            FROM dim_time dt
            WHERE date = CURRENT_DATE
        '
    )
    """,
    engine=engine)
    sundayweeknumber = int(time_df.loc[0, 'sundayweeknumber'])
    print(sundayweeknumber)

    # Save to CSV for inspection
    df.to_csv("assets\\ad_lids_inventory.csv", index=False)


    # ---- NEW: summarize 3 relevant folders ----
    tws = ThreeWeekSummarizer(graph_client=gc, owner_upn=cfg.OWNER_UPN, inventory_df=df)
    results = tws.run(sundayweeknumber=sundayweeknumber, horizon=3)

    # Example: write each folder’s per-file DataFrames to disk (one Excel workbook per folder)
    # Each sheet is a file; DataFrame has a 'SourceFile' column for traceability.
    import os
    from pathlib import Path
    import pandas as pd

    out_dir = Path("assets") / "summaries"
    out_dir.mkdir(parents=True, exist_ok=True)

    for folder_name, files_map in results.items():
        if not files_map:
            continue
        safe_folder = folder_name.replace("/", "_")
        out_path = out_dir / f"{safe_folder}.xlsx"
        with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
            # Manifest (from inventory) for context
            folder_files = tws.resolver.files_under_folder_name(folder_name)
            folder_files.to_excel(xw, index=False, sheet_name="__manifest__")

            for fname, df_file in files_map.items():
                sheet = fname[:31] or "Sheet"  # Excel sheet name limit
                df_file.to_excel(xw, index=False, sheet_name=sheet)

    logger.info("Summaries written to: %s", out_dir.resolve())

if __name__ == "__main__":
    main()
