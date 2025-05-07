import pandas as pd, pathlib as pl, pprint as pp
for f in pl.Path("data/cleaned").glob("*.parquet"):
    cols = pd.read_parquet(f).columns.tolist()
    print(f"{f.name} →")
    pp.pp(cols)
