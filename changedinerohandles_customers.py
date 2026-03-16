import pandas as pd
import warnings
import logging
import os

# -------------------------------------
# Setup
# -------------------------------------
warnings.simplefilter(action='ignore', category=FutureWarning)

# ---- Configuration ----
INPUT_FILE = 'customers.csv'
OUTPUT_EXCEL = 'updated_customers.xlsx'
OUTPUT_CASE_HANDLE = 'case_id_to_handle.txt'
OUTPUT_CASE_MAPPING = 'case_id_to_lowest_id.txt'
OUTPUT_CASE_CUSTOMER_NUMBER = 'case_customer_number_to_customer_number.txt'

DEFAULT_HANDLE = 'unik'
MAX_ROWS_PER_FILE = 1200

COL_ID = 'id'
COL_NAME = 'customer_name'
COL_ROADNAME = 'customer_roadname'
COL_CUSTOMER_NUMBER = 'customer_number'
COL_HANDLE = 'dinero_handle'

# -------------------------------------
# Logging setup
# -------------------------------------
LOG_FILE = "processing_log.txt"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if logger.hasHandlers():
    logger.handlers.clear()

console_handler = logging.StreamHandler()
file_handler = logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

# -------------------------------------
# Load file
# -------------------------------------
file_ext = os.path.splitext(INPUT_FILE)[1].lower()

if file_ext == ".xlsx":
    df = pd.read_excel(INPUT_FILE, engine="openpyxl")

elif file_ext == ".csv":
    logger.info("Loading CSV file...")
    df = pd.read_csv(
        INPUT_FILE,
        sep=";",
        encoding="cp1252",
        engine="python",
        on_bad_lines="warn"
    )
else:
    raise ValueError(
        f"Unsupported file type '{file_ext}'. Only .xlsx and .csv are supported."
    )

# -------------------------------------
# Validate structure
# -------------------------------------
required_columns = {
    COL_ID,
    COL_NAME,
    COL_ROADNAME,
    COL_CUSTOMER_NUMBER,
    COL_HANDLE
}

missing = required_columns - set(df.columns)
if missing:
    raise ValueError(f"Missing required columns: {missing}")

df[COL_ID] = pd.to_numeric(df[COL_ID], errors='raise')
df[COL_CUSTOMER_NUMBER] = pd.to_numeric(df[COL_CUSTOMER_NUMBER], errors='raise')

if df[COL_ID].duplicated().any():
    raise ValueError("Duplicate IDs found.")

logger.info(f"Loaded {len(df)} rows from {INPUT_FILE}")

# -------------------------------------
# Normalize grouping keys
# -------------------------------------
df['_norm_name'] = df[COL_NAME].astype(str).str.strip()
df['_norm_roadname'] = df[COL_ROADNAME].astype(str).str.strip()

# -------------------------------------
# Process groups
# -------------------------------------
group_count = 0
duplicate_count = 0
null_assignments = 0

def process_group(group: pd.DataFrame) -> pd.DataFrame:
    global group_count, duplicate_count, null_assignments

    group_count += 1

    if len(group) > 1:
        duplicate_count += len(group) - 1

    group_sorted = group.sort_values(COL_ID)

    lowest_id = group_sorted.iloc[0][COL_ID]
    canonical_customer_number = group_sorted.iloc[0][COL_CUSTOMER_NUMBER]

    non_null_handles = group_sorted[COL_HANDLE].dropna()

    if not non_null_handles.empty:
        canonical_handle = non_null_handles.iloc[0]
    else:
        canonical_handle = DEFAULT_HANDLE

    group.loc[group[COL_ID] == lowest_id, COL_HANDLE] = canonical_handle

    mask_duplicates = group[COL_ID] != lowest_id
    null_assignments += mask_duplicates.sum()
    group.loc[mask_duplicates, COL_HANDLE] = None

    group['lowest_id'] = lowest_id
    group['canonical_customer_number'] = canonical_customer_number

    return group


df = (
    df.groupby(['_norm_name', '_norm_roadname'], group_keys=False)
      .apply(process_group)
      .reset_index(drop=True)
)

df = df.drop(columns=['_norm_name', '_norm_roadname'])

# -------------------------------------
# Final sort
# -------------------------------------
df = df.sort_values(
    [COL_NAME, COL_ROADNAME, COL_ID]
).reset_index(drop=True)

# -------------------------------------
# Build SQL fragments
# -------------------------------------
def handle_case_row(row):
    if pd.isna(row[COL_HANDLE]):
        return f"WHEN {row[COL_ID]} THEN NULL"
    return f"WHEN {row[COL_ID]} THEN '{row[COL_HANDLE]}'"

df['id_to_handle'] = df.apply(handle_case_row, axis=1)

df['customer_number_to_canonical'] = df.apply(
    lambda row: (
        f"WHEN {row[COL_CUSTOMER_NUMBER]} "
        f"THEN {row['canonical_customer_number']}"
    ),
    axis=1
)

# -------------------------------------
# Helpers
# -------------------------------------
def chunk_dataframe(df, size):
    for start in range(0, len(df), size):
        yield df.iloc[start:start + size]

# -------------------------------------
# Writers
# -------------------------------------
def write_handle_files(df):

    for i, chunk in enumerate(chunk_dataframe(df, MAX_ROWS_PER_FILE), start=1):

        ids_str = ', '.join(str(x) for x in chunk[COL_ID])
        case_body = '\n'.join(chunk['id_to_handle'])

        sql = (
            f"SET {COL_HANDLE} = CASE {COL_ID}\n"
            + case_body
            + f"\nELSE {COL_HANDLE}\nEND\n"
            + f"WHERE {COL_ID} IN ({ids_str});"
        )

        filename = OUTPUT_CASE_HANDLE.replace(".txt", f"_{i}.txt")

        with open(filename, 'w', encoding='utf-8') as f:
            f.write(sql)

        logger.info(f"Wrote {filename} with {len(chunk)} rows")


def write_lowest_id_files(df):

    for i, chunk in enumerate(chunk_dataframe(df, MAX_ROWS_PER_FILE), start=1):

        ids_str = ', '.join(str(x) for x in chunk[COL_ID])

        case_body = '\n'.join(
            f"WHEN {row[COL_ID]} THEN {row['lowest_id']}"
            for _, row in chunk.iterrows()
        )

        sql = (
            f"CASE customer_id\n"
            + case_body
            + f"\nELSE customer_id\nEND\n"
            + f"WHERE customer_id IN ({ids_str});"
        )

        filename = OUTPUT_CASE_MAPPING.replace(".txt", f"_{i}.txt")

        with open(filename, 'w', encoding='utf-8') as f:
            f.write(sql)

        logger.info(f"Wrote {filename} with {len(chunk)} rows")


def write_customer_number_case_files(df):

    unique_df = df.drop_duplicates(subset=[COL_CUSTOMER_NUMBER])

    for i, chunk in enumerate(chunk_dataframe(unique_df, MAX_ROWS_PER_FILE), start=1):

        nums = ', '.join(str(x) for x in chunk[COL_CUSTOMER_NUMBER])
        case_body = '\n'.join(chunk['customer_number_to_canonical'])

        sql = (
            f"SET {COL_CUSTOMER_NUMBER} = CASE {COL_CUSTOMER_NUMBER}\n"
            + case_body
            + f"\nELSE {COL_CUSTOMER_NUMBER}\nEND\n"
            + f"WHERE {COL_CUSTOMER_NUMBER} IN ({nums});"
        )

        filename = OUTPUT_CASE_CUSTOMER_NUMBER.replace(".txt", f"_{i}.txt")

        with open(filename, 'w', encoding='utf-8') as f:
            f.write(sql)

        logger.info(f"Wrote {filename} with {len(chunk)} rows")

# -------------------------------------
# Write outputs
# -------------------------------------
df.to_excel(OUTPUT_EXCEL, index=False)

write_handle_files(df)
write_lowest_id_files(df)
write_customer_number_case_files(df)

# -------------------------------------
# Logging summary
# -------------------------------------
unique_customers_with_handle = df[df[COL_HANDLE].notna()][COL_ID].nunique()

logger.info(f"Total rows processed: {len(df)}")
logger.info(f"Total groups processed: {group_count}")
logger.info(f"Total duplicates collapsed: {duplicate_count}")
logger.info(f"Total NULL assignments: {null_assignments}")
logger.info(f"Unique customers (NOT NULL {COL_HANDLE}): {unique_customers_with_handle}")
logger.info("Processing complete.")
logger.info(f"Log written to: {LOG_FILE}")
