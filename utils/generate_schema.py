"""
Generate a complete SQL‑Server DDL script that matches the columns
and dtypes actually present in each *.parquet file under data/cleaned/.

Usage:
    python -m utils.generate_schema
    # → writes schema_bank.sql beside the script
"""

import pathlib as pl
import pandas as pd
from textwrap import indent

PARQUET_DIR = pl.Path("data/cleaned")
DB_NAME = "BankingAnalytics"

# Map pandas -> SQL Server (very simple; adjust if you need more finesse)
TYPE_MAP = {
    "int64":        "BIGINT",
    "int32":        "INT",
    "float64":      "DECIMAL(18,2)",
    "bool":         "BIT",
    "datetime64[ns]": "DATETIME2",
    "object":       "NVARCHAR(4000)"
}

def to_sql_type(pd_series):
    return TYPE_MAP.get(str(pd_series.dtype), "NVARCHAR(4000)")

def build_table(name, df):
    lines = []
    for col in df.columns:
        sql_type = to_sql_type(df[col])
        lines.append(f"    [{col}] {sql_type}")
    # crude primary key assumption: 'id' present? else none
    pk = ",\n    PRIMARY KEY ([id])" if "id" in df.columns else ""
    body = ",\n".join(lines) + pk
    return f"CREATE TABLE dbo.[{name}] (\n{body}\n);\n"

def main():
    ddl_parts = [
        f"USE master;\n"
        f"IF DB_ID(N'{DB_NAME}') IS NOT NULL BEGIN\n"
        f"    ALTER DATABASE {DB_NAME} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;\n"
        f"    DROP DATABASE {DB_NAME};\nEND;\n"
        f"CREATE DATABASE {DB_NAME};\nGO\nUSE {DB_NAME};\nGO\n"
    ]

    for pq_file in PARQUET_DIR.glob("*.parquet"):
        name = pq_file.stem            # users.parquet -> users
        df = pd.read_parquet(pq_file)
        ddl_parts.append(build_table(name, df))
        ddl_parts.append("GO\n")

    # retrieveinfo lineage table
    ddl_parts.append(
        "CREATE TABLE dbo.retrieveinfo (\n"
        "    retrieve_id  INT IDENTITY PRIMARY KEY,\n"
        "    source_file  NVARCHAR(64),\n"
        "    retrieved_at DATETIME2  DEFAULT SYSDATETIME(),\n"
        "    total_rows   INT,\n"
        "    processed_rows INT,\n"
        "    errors       INT,\n"
        "    notes        NVARCHAR(MAX)\n);\nGO\n"
    )

    out = pl.Path("schema_bank.sql")
    out.write_text("".join(ddl_parts), encoding="utf‑8")
    print(f"✅  Wrote {out.resolve()}")

if __name__ == "__main__":
    main()
