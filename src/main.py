# main.py
from __future__ import annotations

import logging

from modules.auth import AuthProvider
from modules.config import Config
from modules.graph_client import GraphClient
from modules.query import OneDriveFolderQuery, SQLServerQuery

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
    print(time_df.loc[0, 'sundayweeknumber'])

    # Save to CSV for inspection
    df.to_csv("assets\\ad_lids_inventory.csv", index=False)

if __name__ == "__main__":
    main()
