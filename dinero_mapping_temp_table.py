import pandas as pd
import warnings
import logging
import os
warnings.simplefilter(action="ignore", category=FutureWarning)
# --------------------------------------------------
# Configuration
# --------------------------------------------------
INPUT_FILE = "customers.csv"
OUTPUT_MAPPING = "customer_mapping.csv"
COL_ID = "id"
COL_NAME = "customer_name"
COL_ROADNAME = "customer_roadname"
COL_CUSTOMER_NUMBER = "customer_number"
COL_HANDLE = "dinero_handle"
DEFAULT_HANDLE = "unik"
# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)
# --------------------------------------------------
# Load file
# --------------------------------------------------
ext = os.path.splitext(INPUT_FILE)[1].lower()
if ext == ".xlsx":
    df = pd.read_excel(INPUT_FILE, engine="openpyxl")
elif ext == ".csv":
    df = pd.read_csv(
        INPUT_FILE,
        sep=";",
        encoding="cp1252",
        engine="python",
        on_bad_lines="warn"
    )
else:
    raise ValueError("Unsupported file type")
logger.info(f"Loaded {len(df)} rows")
# --------------------------------------------------
# Normalize column headers
# --------------------------------------------------
df.columns = (
    df.columns
    .str.strip()
    .str.replace('"', '', regex=False)
    .str.lower()
)
logger.info(f"Columns detected: {df.columns.tolist()}")
# --------------------------------------------------
# Validate required columns
# --------------------------------------------------
required = {COL_ID, COL_NAME, COL_ROADNAME, COL_CUSTOMER_NUMBER, COL_HANDLE}
missing = required - set(df.columns)
if missing:
    raise ValueError(f"Missing columns: {missing}")
# --------------------------------------------------
# Type conversions
# --------------------------------------------------
df[COL_ID] = pd.to_numeric(df[COL_ID], errors="raise")
df[COL_CUSTOMER_NUMBER] = pd.to_numeric(df[COL_CUSTOMER_NUMBER], errors="raise")
# --------------------------------------------------
# Normalize grouping keys
# --------------------------------------------------
df["_norm_name"] = df[COL_NAME].astype(str).str.strip().str.lower()
df["_norm_roadname"] = df[COL_ROADNAME].astype(str).str.strip().str.lower()
# --------------------------------------------------
# Process duplicate groups
# --------------------------------------------------
def process_group(group: pd.DataFrame) -> pd.DataFrame:
    group = group.sort_values(COL_ID)
    lowest_id = group.iloc[0][COL_ID]
    canonical_customer_number = group.iloc[0][COL_CUSTOMER_NUMBER]
    non_null_handles = group[COL_HANDLE].dropna()
    if not non_null_handles.empty:
        canonical_handle = non_null_handles.iloc[0]
    else:
        canonical_handle = DEFAULT_HANDLE
    group["canonical_customer_id"] = lowest_id
    group["canonical_customer_number"] = canonical_customer_number
    # Only canonical keeps handle
    group.loc[group[COL_ID] == lowest_id, "final_handle"] = canonical_handle
    group.loc[group[COL_ID] != lowest_id, "final_handle"] = None
    return group
df = (
    df.groupby(["_norm_name", "_norm_roadname"], group_keys=False)
      .apply(process_group)
      .reset_index(drop=True)
)
# --------------------------------------------------
# Build mapping (KEEP original customer_number)
# --------------------------------------------------
mapping = df.rename(columns={
    COL_ID: "customer_id",
    COL_CUSTOMER_NUMBER: "customer_number"
})[[
    "customer_id",
    "customer_number",                # ⭐ original number kept
    "canonical_customer_id",
    "canonical_customer_number",
    "final_handle"
]].rename(columns={
    "final_handle": "dinero_handle"
})
# Replace NaN with NULL
mapping["dinero_handle"] = mapping["dinero_handle"].where(
    mapping["dinero_handle"].notna(), None
)
# --------------------------------------------------
# Sort so canonical appears first inside group
# --------------------------------------------------
mapping["is_canonical"] = (
    mapping["customer_id"] == mapping["canonical_customer_id"]
)
mapping = (
    mapping
    .sort_values(
        ["canonical_customer_id", "is_canonical", "customer_id"],
        ascending=[True, False, True]
    )
    .drop(columns="is_canonical")
    .reset_index(drop=True)
)
# --------------------------------------------------
# Save CSV
# --------------------------------------------------
mapping.to_csv(
    OUTPUT_MAPPING,
    index=False,
    sep=";",
    encoding="cp1252"
)
logger.info(f"Mapping file written: {OUTPUT_MAPPING}")
logger.info(f"Rows: {len(mapping)}")