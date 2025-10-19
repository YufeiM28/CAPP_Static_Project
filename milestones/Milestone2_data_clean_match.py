import pandas as pd
import re
from pathlib import Path
import sys

# Data Cleaning
df = pd.read_csv("~/Desktop/school profile/Profile_1819.csv")
print(df.columns)

highschools1819_df = df[
    (df["Is_High_School"] == True) &
    (df["Is_Elementary_School"] == False) &
    (df["Is_Middle_School"] == False)]
print("Number of high schools:", len(highschools1819_df))

output_path = "Progress_1819_highschools.csv"
highschools1819_df.to_csv(output_path, index=False)

# Data Matching
paths = {
    "clean_1819": "~/PyCharmMiscProject/Progress_1819_highschools.csv",
    "progress_1112": "~/Desktop/school progress/Progress_11-12_.csv",
    "profile_2324": "~/Desktop/school profile/Profile_2324.csv",
    "progress_1819": "~/Desktop/school progress/Progress_1819.csv",
    "progress_2324": "~/Desktop/school progress/Progress_2324.csv"}
outdir = Path("~/Desktop").expanduser()
outdir.mkdir(parents=True, exist_ok=True)

COMMON_ID_NAMES = ["School_ID", "School ID", "SchoolID"]

def guess_id_column(columns):
    """Find a school ID column by name heuristics."""
    normalized = {c: re.sub(r"[\s_]+", "", c).lower() for c in columns}
    targets = [re.sub(r"[\s_]+", "", n).lower() for n in COMMON_ID_NAMES]
    # exact match first
    for c, norm in normalized.items():
        if norm in targets:
            return c
    # 'school' + 'id' anywhere
    for c, norm in normalized.items():
        if 'school' in norm and 'id' in norm:
            return c
    # literal 'id'
    for c in columns:
        if c.strip().lower() == "id":
            return c
    return None

def read_csv_safely(path_like):
    p = Path(path_like).expanduser()
    return pd.read_csv(str(p), dtype=str, low_memory=False)

def normalize_id_series(s):
    s = s.astype(str).str.strip().str.replace(r"^['\"]|['\"]$", "", regex=True).str.upper()
    s = s.str.replace(r"\s+", "", regex=True)
    return s

missing = []
for name, p in paths.items():
    q = Path(p).expanduser()
    if not q.exists():
        missing.append((name, str(q)))
if missing:
    print("[ERROR] The following files were not found:")
    for name, q in missing:
        print(f"  - {name}: {q}")
    print("\nFix the paths above (note: '~' expands to your home directory) and rerun.")
    sys.exit(1)

dfs = {name: read_csv_safely(p) for name, p in paths.items()}
detected = {}
for name, df in dfs.items():
    id_col = guess_id_column(df.columns)
    detected[name] = id_col
    print(f"[INFO] Detected ID column in {name}: {id_col}")

clean_df = dfs["clean_1819"].copy()
clean_id_col = detected["clean_1819"]
if clean_id_col is None:
    raise ValueError(
        "Could not detect School ID column in the 2018–19 cleaned dataset.\n"
        f"Columns: {list(clean_df.columns)}\n"
        "Tip: set COMMON_ID_NAMES to your exact column name.")
clean_df["_ID_NORM"] = normalize_id_series(clean_df[clean_id_col])
clean_ids = set(clean_df["_ID_NORM"].dropna())
print(f"[INFO] Unique IDs in cleaned 2018–19: {len(clean_ids)}")
if not clean_ids:
    raise ValueError("No IDs found")


for name in list(paths.keys()):
    if name == "clean_1819":
        continue

    df = dfs[name].copy()
    id_col = detected[name]

    if id_col is None or id_col not in df.columns:
        print(f"[WARNING] Could not find a valid ID column in {name}.")
        print(f"          Columns (sample): {list(df.columns)[:12]} ...")
        continue
    print(f"[INFO] Filtering {name} using ID column: {id_col}")



    df["_ID_NORM"] = normalize_id_series(df[id_col])
    print(f"[DEBUG] Created _ID_NORM for {name} with {df['_ID_NORM'].nunique()} unique IDs")

    filtered = df[df["_ID_NORM"].isin(clean_ids)].copy()
    print(f"[INFO] {name}: kept {len(filtered)} of {len(df)} rows")

    # save
    filtered.drop(columns=["_ID_NORM"], inplace=True, errors="ignore")
    outpath = outdir / f"{name}_filtered_by_1819_cleaned_ids.csv"
    filtered.to_csv(outpath, index=False)
    print(f"[SAVED] {outpath}")

ids_out = outdir / "Matching_School_IDs_from_1819_cleaned.csv"
pd.DataFrame(sorted(clean_ids), columns=["School_ID_Normalized"]).to_csv(ids_out, index=False)
print(f"[SAVED] {ids_out}")
print("Done.")
