"""
STEP 1 : Decode raw banking CSV files.

Usage (from project root):
    python -m src.decode
"""

import json
import pandas as pd
import pathlib as pl
from typing import Dict


# ---------- configuration ---------- #
RAW_DIR   = pl.Path("data/raw")                 # where the encoded files live
OUT_DIR   = pl.Path("data/clean")               # decoded output goes here
MAP_FILE  = RAW_DIR / "column_table_map.json"   # provides the rename rules
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------- helpers ---------- #
def load_column_map() -> Dict[str, dict]:
    """Return the mapping dict keyed by table‑ID ('01', '02', …)."""
    with MAP_FILE.open(encoding="utf‑8") as fh:
        return json.load(fh)


def decode_table(table_id: str, meta: dict) -> pd.DataFrame:
    """
    Read tXX.csv, rename its columns to human names and return a DataFrame.
    `meta` is the sub‑dict for that table inside column_table_map.json.
    """
    csv_path = RAW_DIR / meta["file"]
    df = pd.read_csv(csv_path, dtype=str)          # keep raw strings first

    # Build rename mapping: "01‑03"  →  "user_name"   (example)
    rename = {f"{table_id}-{k}": v for k, v in meta["columns"].items()}
    df = df.rename(columns=rename)

    df["source_file"] = meta["file"]                # lineage
    return df


def main() -> None:
    column_map = load_column_map()

    for table_id, meta in column_map.items():
        csv_path = RAW_DIR / meta["file"]
        if not csv_path.exists():
            print(f"⚠  {meta['file']} not found – skipping")
            continue

        df = decode_table(table_id, meta)
        out_path = OUT_DIR / f"{meta['table']}.parquet"
        df.to_parquet(out_path, index=False)
        print(f"✓ decoded {meta['file']} → {out_path}  ({len(df):,} rows)")


if __name__ == "__main__":
    main()
