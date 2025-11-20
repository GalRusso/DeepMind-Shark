from databricks.sdk.runtime import dbutils, spark
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SUPPORTED_MAJOR_TYPES = {"image", "audio", "video"}


dbutils.widgets.text("ENVIRONMENT", "dev")
dbutils.widgets.text("vertical", "deepmind_labeling")
dbutils.widgets.text("data_domain", "ggdrive")
#test

ENVIRONMENT = dbutils.widgets.get("ENVIRONMENT")
VERTICAL = dbutils.widgets.get("vertical")
SOURCE = dbutils.widgets.get("data_domain")

CATALOG = f"af_delivery_{ENVIRONMENT}"
NAMESPACE = f"{CATALOG}.ingestion_{VERTICAL}"
SHEET_SNAPSHOT_TABLE = f"{NAMESPACE}.gsheet_snapshots"
FILES_TABLE = f"{NAMESPACE}.ggdrive_files"
TRACKING_TABLE = f"{NAMESPACE}.ggdrive_files_download_tracking"


SNAPSHOT_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),  # uuid
        StructField("sheet_drive_id", StringType(), False, metadata={"comment": "Google Sheets spreadsheet ID"}),
        StructField(
            "sheet_revision_id", StringType(), False, metadata={"comment": "Google Sheets spreadsheet revision ID"}
        ),
        StructField("sheet_snapshot_ts", TimestampType(), False),
        StructField("sheet_snapshot_file_path", StringType(), False),
        StructField("sheet_snapshot_row_count", LongType(), False),
    ]
)
FILES_SCHEMA = StructType(
    [
        # links with the snaptshot sheet
        StructField("sheet_id", StringType(), False),
        # keys provided by the snapshot sheet
        StructField("source_url", StringType(), False, metadata={"comment": "URL of the file, given by the sheet"}),
        StructField("file_title", StringType(), False, metadata={"comment": "Title of the file, given by the sheet"}),
        StructField("file_hash", StringType(), False, metadata={"comment": "MD5 hash of the file, given by the sheet"}),
        # computed fields
        StructField("source_id", StringType(), False, metadata={"comment": "Google Drive file ID"}),
    ]
)

TRACKING_SCHEMA = StructType(
    [
        StructField("sheet_id", StringType(), False),
        StructField("source_id", StringType(), True, metadata={"comment": "Google Drive file ID"}),
        StructField("revision_id", StringType(), True, metadata={"comment": "Google Drive file revision ID"}),
        StructField("gdrive_path", StringType(), True, metadata={"comment": "Google Drive path"}),
        StructField(
            "original_path",
            StringType(),
            True,
            metadata={"comment": "Path after landing, e.g. 's3://...<file_name>.docx'"},
        ),
        # a json file with metadata, e.g. "s3://...<file_name>.docx.metadata.json"
        StructField("metadata_path", StringType(), True, metadata={"comment": "Path to the metadata file"}),
        StructField("error", StringType(), True, metadata={"comment": "Error message"}),
        StructField(
            "ts_downloaded",
            TimestampType(),
            True,
            metadata={"comment": "When artifacts were successfully written to s3"},
        ),
    ]
)

spark.createDataFrame([], schema=SNAPSHOT_SCHEMA).write.format("delta").mode("ignore").saveAsTable(SHEET_SNAPSHOT_TABLE)
spark.createDataFrame([], schema=FILES_SCHEMA).write.format("delta").mode("ignore").saveAsTable(FILES_TABLE)
spark.createDataFrame([], schema=TRACKING_SCHEMA).write.format("delta").mode("ignore").saveAsTable(TRACKING_TABLE)
