from sqlalchemy import create_engine, inspect
import pandas as pd, pathlib as pl

# ---- edit these credentials once ------------------------------------------
ENG = create_engine(
    r"mssql+pyodbc://HARVEY\SQLEXPRESS/BankingAnalytics?driver=ODBC+Driver+17+for+SQL+Server"
)
# ---------------------------------------------------------------------------

insp = inspect(ENG)
clean_dir = pl.Path("data/cleaned")

for tbl in insp.get_table_names(schema="dbo"):
    db_cols = [c["name"] for c in insp.get_columns(tbl)]
    pq_file = clean_dir / f"{tbl}.parquet"
    if not pq_file.exists():
        print(f"⚠  {tbl}: no matching parquet file – skipped")
        continue

    pq_cols = pd.read_parquet(pq_file).columns.tolist()
    diff = set(pq_cols) ^ set(db_cols)

    if diff:
        print(f"❌  {tbl}: mismatch → {diff}")
    else:
        print(f"✓ {tbl}: columns match")
