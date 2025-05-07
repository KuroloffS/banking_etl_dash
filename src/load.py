"""
STEP 4 : Bulk‑load cleaned Parquet tables into SQL Server
          and log each ingestion into dbo.retrieveinfo.

Usage:
    python -m src.load
"""

import pathlib as pl
import pandas as pd
from sqlalchemy import create_engine, text

# ---------- CONFIG ---------------------------------------------------
DATA_DIR = pl.Path("data/cleaned")
TABLES   = [
    "users",
    "cards",
    "transactions",
    "logs",
    "reports",
    "scheduled_payments",
]

ENGINE = create_engine(
    r"mssql+pyodbc://HARVEY\SQLEXPRESS/BankingAnalytics"
    r"?driver=ODBC Driver 17 for SQL Server"
    r"&trusted_connection=yes",
    connect_args={"fast_executemany": True} 
)
# --------------------------------------------------------------------

def load_one(table: str) -> None:
    path = DATA_DIR / f"{table}.parquet"
    df   = pd.read_parquet(path)

    # Insert data
    df.to_sql(table, ENGINE, if_exists="replace", index=False, chunksize=1000)

    # Count rows with any *_issue flagged
    issue_cols = [c for c in df.columns if c.endswith("_issue")]
    error_cnt  = 0 if not issue_cols else df[issue_cols].notna().any(axis=1).sum()

    with ENGINE.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO dbo.retrieveinfo
                       (source_file, total_rows, processed_rows, errors, notes)
                VALUES (:src, :tot, :proc, :err, 'loaded by src.load')
                """
            ),
            {
            "src":   path.name,
            "tot":   int(len(df) + error_cnt),   # ← cast to int
            "proc":  int(len(df)),               # ← cast to int
            "err":   int(error_cnt),             # ← cast to int
            },
        )

    print(f"✓ loaded {table:<20} {len(df):>6,} rows  (errors={error_cnt})")

def main():
    for tbl in TABLES:
        load_one(tbl)

if __name__ == "__main__":
    main()
