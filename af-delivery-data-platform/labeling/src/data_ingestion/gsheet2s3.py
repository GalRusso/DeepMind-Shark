# %%
import argparse
import datetime
import json
import re
import sys
import uuid

from constants import (
    ENVIRONMENT,
    FILES_TABLE,
    SHEET_SNAPSHOT_TABLE,
    SNAPSHOT_SCHEMA,
    SOURCE,
    VERTICAL,
)
from databricks.sdk.runtime import dbutils, spark
from delta.tables import DeltaTable
from pyspark.sql.functions import col, udf
from pyspark.sql.types import Row, StringType

sys.path.append("../../../shared")

from libs.google_sheet import get_sheet_values, get_sheet_version

# %%

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--google_sheets_spreadsheet_id", default="1ajkkj72FUyPrgRY5opJ8hh4NiLe8yAoGH1UBn-sMJis")
parser.add_argument("--google_sheets_sheet_name", default="Screening")
parser.add_argument("--file_name_column", default="Title")
parser.add_argument("--external_url_column", default="[TK] Media File URL")
parser.add_argument("--file_hash_column", default="Hash (ID)")

# Use parse_known_args to avoid errors if Databricks injects extra args
args, _ = parser.parse_known_args()

GOOGLE_SHEETS_SPREADSHEET_ID = args.google_sheets_spreadsheet_id
GOOGLE_SHEETS_SHEET_NAME = args.google_sheets_sheet_name
FILE_NAME_COLUMN = args.file_name_column
EXTERNAL_URL_COLUMN = args.external_url_column
FILE_HASH_COLUMN = args.file_hash_column

REDO = False
SERVICE_ACCOUNT_DATA = json.loads(dbutils.secrets.get("datn", "google-service-account-json"))

# %%


def dir_path(kind: str):
    domain = kind
    now = datetime.datetime.now()
    return f"/Volumes/af_delivery_{ENVIRONMENT}/data_collection/{VERTICAL}/{domain}/{SOURCE}/{now:%Y}/{now:%m}/{now:%d}"


# %%
# FIXME: this still not good enough. The fingerprint still changes.
sheet_revision_id = get_sheet_version(
    SERVICE_ACCOUNT_DATA, GOOGLE_SHEETS_SPREADSHEET_ID
)  # Now using Drive version as fingerprint

# %%
df = get_sheet_values(SERVICE_ACCOUNT_DATA, GOOGLE_SHEETS_SPREADSHEET_ID, GOOGLE_SHEETS_SHEET_NAME)
df.head(2)

# %%
csv_string = df.to_csv(index=False)
dest_dir = dir_path("ggdrive_sheet")
now = datetime.datetime.now()
dest_path = f"{dest_dir}/sheet__{GOOGLE_SHEETS_SPREADSHEET_ID}__{sheet_revision_id}__{now:%H%M%S}.csv"
row_count = len(df)

# %%
existing_snapshots = spark.table(SHEET_SNAPSHOT_TABLE).filter(col("sheet_revision_id") == sheet_revision_id)
if existing_snapshots.count() > 0:
    print(f"Sheet snapshot already ingested. revision_id={sheet_revision_id}. No-op.")
    exit(0)

print(f"Sheet snapshot not ingested. revision_id={sheet_revision_id}. Proceeding.")

# %%
dbutils.fs.mkdirs(dest_dir)
dbutils.fs.put(dest_path, csv_string, overwrite=True)

new_snapshot_row = Row(
    id=str(uuid.uuid4()),
    sheet_drive_id=GOOGLE_SHEETS_SPREADSHEET_ID,
    sheet_revision_id=sheet_revision_id,
    sheet_snapshot_ts=now,
    sheet_snapshot_file_path=dest_path,
    sheet_snapshot_row_count=row_count,
)
new_snapshot_df = spark.createDataFrame([new_snapshot_row], schema=SNAPSHOT_SCHEMA)

# Use Delta MERGE to upsert snapshot
snapshot_delta_table = DeltaTable.forName(spark, SHEET_SNAPSHOT_TABLE)
snapshot_delta_table.alias("existing").merge(
    new_snapshot_df.alias("new"), "existing.sheet_revision_id = new.sheet_revision_id"
).whenNotMatchedInsertAll().execute()

# INSERT INTO GDRIVE_FILES
body = df.copy()
body = body.reset_index(drop=True)
# Ensure row_index is an integer type to match the table schema

body["sheet_id"] = GOOGLE_SHEETS_SPREADSHEET_ID
body["source_url"] = body[EXTERNAL_URL_COLUMN]
body["file_title"] = body[FILE_NAME_COLUMN]
body["file_hash"] = body[FILE_HASH_COLUMN]


def url_to_file_id(drive_url: str) -> str | None:
    match = re.search(r"/d/([a-zA-Z0-9_-]+)", drive_url)
    if not match:
        print(f"Could not extract file id from drive url {drive_url}")
        return None
    return match.group(1)


manifest_pdf = body[
    [
        "sheet_id",
        "source_url",
        "file_title",
        "file_hash",
    ]
].copy()

# Convert to Spark DataFrame first, then apply transformation using UDF
url_to_file_id_udf = udf(url_to_file_id, StringType())

manifest_sdf_temp = spark.createDataFrame(manifest_pdf)
manifest_sdf_temp = manifest_sdf_temp.withColumn("source_id", url_to_file_id_udf(col("source_url")))
manifest_sdf_temp = manifest_sdf_temp.where(col("source_id").isNotNull())

# Select final columns with correct schema
manifest_sdf = manifest_sdf_temp.select(
    col("sheet_id"), col("source_url"), col("file_title"), col("file_hash"), col("source_id")
)

# Use Delta MERGE to upsert
delta_table = DeltaTable.forName(spark, FILES_TABLE)

delta_table.alias("t").merge(
    manifest_sdf.alias("u"), "t.source_id = u.source_id AND t.file_hash = u.file_hash"
).whenNotMatchedInsertAll().execute()

print(f"Snapshot written to {dest_path}; manifest merged for {row_count} rows. MD5={sheet_revision_id}")
