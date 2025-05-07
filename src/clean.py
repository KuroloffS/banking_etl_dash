"""
STEP 2 : Clean + validate Parquet tables.

Run:
    python -m src.clean
"""

import re, sys, pathlib as pl, pandas as pd

SRC_DIR = pl.Path("data/clean")     # decoded output from stage 1
OUT_DIR = pl.Path("data/cleaned")   # final output for stage 3
OUT_DIR.mkdir(exist_ok=True)

# --- common regexes ----------------------------------------------------------
PHONE_RE = re.compile(r"^\+?\d{9,15}$")       # naive example
EMAIL_RE = re.compile(r"^[^@]+@[^@]+\.[^@]+$")

# --- helper to add *_issue columns -------------------------------------------
def flag(df, col, pattern, label):
    bad = ~df[col].fillna("").str.match(pattern)
    df.loc[bad, f"{col}_issue"] = label
    return df

# ---------------------- cleaners ---------------------------------------------
def clean_users() -> pd.DataFrame:
    df = pd.read_parquet(SRC_DIR / "users.parquet")

    df["created_at"]    = pd.to_datetime(df["created_at"], errors="coerce")
    df["total_balance"] = pd.to_numeric(df["total_balance"], errors="coerce")
    df["is_vip"] = df["total_balance"] > 10_000

    df = flag(df, "phone_number", PHONE_RE, "bad_format")
    df = flag(df, "email", EMAIL_RE, "bad_format")
    return df


def clean_cards() -> pd.DataFrame:
    df = pd.read_parquet(SRC_DIR / "cards.parquet")

    # numeric columns
    for c in ["balance", "limit_amount"]:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # --- detect the “blocked?” flag -----------------------------------------
    # common variants found in t02 mappings
    CANDIDATES = ["is_blocked", "blocked", "card_status", "status"]
    flag_col = next((c for c in CANDIDATES if c in df.columns), None)

    if flag_col:
        df["is_blocked"] = (
            df[flag_col]
            .astype(str)
            .str.lower()
            .map({"1": True, "0": False, "true": True, "false": False, "blocked": True, "active": False})
            .fillna(False)
            .astype("bool")
        )
    else:
        # column truly absent → assume all cards are active
        df["is_blocked"] = False

    return df

def clean_transactions() -> pd.DataFrame:
    df = pd.read_parquet(SRC_DIR / "transactions.parquet")
    df["amount"]     = pd.to_numeric(df["amount"], errors="coerce")
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    # flag > 1 day negative balance as example rule
    df["is_flagged"] = df["amount"] < 0
    return df


CLEANERS = {
    "users": clean_users,
    "cards": clean_cards,
    "transactions": clean_transactions,
    "logs": lambda: pd.read_parquet(SRC_DIR / "logs.parquet"),
    "reports": lambda: pd.read_parquet(SRC_DIR / "reports.parquet"),
    "scheduled_payments": lambda: pd.read_parquet(SRC_DIR / "scheduled_payments.parquet"),
}

# ---------------------- main -------------------------------------------------
def main():
    try:
        import pyarrow  # noqa
    except ImportError:
        sys.exit("❌  pyarrow missing – `pip install pyarrow`")

    for name, fn in CLEANERS.items():
        try:
            df = fn()
            out = OUT_DIR / f"{name}.parquet"
            df.to_parquet(out, index=False)
            print(f"✓ cleaned {name} → {out} ({len(df):,} rows)")
        except FileNotFoundError:
            print(f"⚠  {name}.parquet not found – skipping")


if __name__ == "__main__":
    main()
