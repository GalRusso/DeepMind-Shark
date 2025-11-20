from pyspark.sql.types import ArrayType, StringType, StructType, StructField, TimestampType, DateType

# Schema for the latest snapshot table - updated to match CDC table schema
cleansed_latest_table_schema = StructType([
    StructField("trend_name", StringType(), True, metadata={"comment": "Trend Name"}),
    StructField("country", StringType(), True, metadata={"comment": "The country which we identified the trend was from"}),
    StructField("date_reported", DateType(), True, metadata={"comment": "the date when the trend was reported"}),
    StructField("screenshot_media_links", ArrayType(StringType()), True, metadata={"comment": "Actual media link of the screenshots (link to monday media)"}),
    StructField("artifact_link", StringType(), True, metadata={"comment": "link to the artifact"}),
    StructField("host_platform", StringType(), True, metadata={"comment": "google platform where the artifact is hosted"}),
    StructField("artifact_status", StringType(), True, metadata={"comment": "status of the artifact"}),
    StructField("addtional_notes", StringType(), True, metadata={"comment": "additional notes about artifact"}),
    StructField("monday_item_id", StringType(), True, metadata={"comment": "Monday Board Item ID"}),
    StructField("trend_id", StringType(), True, metadata={"comment": "Trend ID"}),
    StructField("external_trend_id", StringType(), True, metadata={"comment": "External Trend ID from client database"}),
    StructField("analyst", StringType(), True, metadata={"comment": "analyst who labels the trend"}),
    StructField("external_artifact_id", StringType(), True, metadata={"comment": "External Artifact ID"}),
    StructField("created_at", TimestampType(), True, metadata={"comment": "The time that the record was created (UTC)"}),
    StructField("last_updated", TimestampType(), True, metadata={"comment": "Last time that the record was updated (UTC)"}),
    # Extra columns
    StructField("ingestion_timestamp", TimestampType(), True, metadata={"comment": "Timestamp of data ingestion"}),
    StructField("source_file", StringType(), True, metadata={"comment": "Source file path"}),
    StructField("processing_date", TimestampType(), True, metadata={"comment": "Date of processing"}),
    StructField("file_name", StringType(), True, metadata={"comment": "Name of the ingested file"}),
])

import dlt
from pyspark.sql.functions import col
from config import cleansed_cdc_table_name, cleansed_latest_table_name

# Create the latest snapshot table with updated configuration
dlt.create_streaming_table(
    name=cleansed_latest_table_name,
    schema=cleansed_latest_table_schema,
    comment="Latest snapshot of each EWD Copies Items US",
    table_properties={
        "quality": "silver",
        "delta.enableRowTracking": "true"
    },
    partition_cols=["created_at"]
)

# Use DLT AUTO CDC for efficient change data capture
dlt.create_auto_cdc_flow(
    target=cleansed_latest_table_name,
    source=cleansed_cdc_table_name,
    keys=["monday_item_id"],
    sequence_by=col("last_updated"),
    apply_as_deletes=None,  # No delete operations in this use case
    except_column_list=["_rescued_data"],  # Include all columns
    stored_as_scd_type=1  # SCD Type 1 - keep only latest version
)
