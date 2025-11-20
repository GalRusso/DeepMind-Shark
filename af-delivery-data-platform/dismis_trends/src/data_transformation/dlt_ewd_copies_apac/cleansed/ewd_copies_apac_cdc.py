from pyspark.sql.types import ArrayType, StringType, StructType, StructField, TimestampType, DateType

cleansed_cdc_table_schema = StructType([
    StructField("trend_name", StringType(), True, metadata={"comment": "Trend Name"}),
    StructField("trend_id", StringType(), True, metadata={"comment": "Trend ID"}),
    StructField("country", StringType(), True, metadata={"comment": "The country which we identified the trend was from"}),
    StructField("date_reported", DateType(), True, metadata={"comment": "the date when the trend was reported"}),
    StructField("screenshot_media_links", ArrayType(StringType()), True, metadata={"comment": "Actual media link of the screenshots (link to monday media)"}),
    StructField("artifact_link", StringType(), True, metadata={"comment": "link to the artifact"}),
    StructField("host_platform", StringType(), True, metadata={"comment": "google platform where the artifact is hosted"}),
    StructField("artifact_status", StringType(), True, metadata={"comment": "status of the artifact"}),
    StructField("addtional_notes", StringType(), True, metadata={"comment": "additional notes about artifact"}),
    StructField("created_at", TimestampType(), True, metadata={"comment": "The time that the record was created (UTC)"}),
    StructField("monday_item_id", StringType(), True, metadata={"comment": "Monday Board Item ID"}),
    StructField("external_trend_id", StringType(), True, metadata={"comment": "External Trend ID from client database"}),
    StructField("analyst", StringType(), True, metadata={"comment": "analyst who labels the trend"}),
    StructField("external_artifact_id", StringType(), True, metadata={"comment": "External Artifact ID"}),
    StructField("last_updated", TimestampType(), True, metadata={"comment": "Last time that the record was updated (UTC)"}),
    # Extra columns
    StructField("_rescued_data", StringType(), True, metadata={"comment": "Rescued data for schema evolution"}),
    StructField("ingestion_timestamp", TimestampType(), True, metadata={"comment": "Timestamp of data ingestion"}),
    StructField("source_file", StringType(), True, metadata={"comment": "Source file path"}),
    StructField("processing_date", TimestampType(), True, metadata={"comment": "Date of processing"}),
    StructField("file_name", StringType(), True, metadata={"comment": "Name of the ingested file"}),
])


import dlt
from pyspark.sql.functions import col, split, trim
from config import landing_cdc_table_name, cleansed_cdc_table_name

dlt.create_streaming_table(
    name=cleansed_cdc_table_name,
    comment="Cleaned and structured EWD APAC copies data with enforced schema",
    schema=cleansed_cdc_table_schema,
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableRowTracking": "true"
    },
    expect_all_or_drop={
        "valid_timestamp": "ingestion_timestamp IS NOT NULL",
        "valid_source": "source_file IS NOT NULL",
        "no_rescued_data": "_rescued_data IS NULL",
        "valid_artifact_status": "artifact_status IN ('Online', 'Offline', 'Restricted')"
    },
    # expect_all_or_fail={
    # }
)

def split_and_cast_as_array(field, pattern):
    return (split(trim(col(field)), pattern).cast(ArrayType(StringType()))).alias(field)

@dlt.append_flow(
    target=cleansed_cdc_table_name,
    comment="Cleaned and structured EWD APAC copies data with enforced schema flow",
)
def landing_to_cleansed_cdc_flow():
    """
    Cleansed layer: Create CDC table optimized for full-refresh data ingestion.
    Processes all data from landing table with proper data type conversions and array splitting.
    Optimized for scenarios where each batch contains the complete dataset.
    This table serves as the source for downstream CDC operations using dlt.create_auto_cdc_flow.
    """
    select_exprs = [
        col("name").alias("trend_name"),
        col("ewd_trends_apac")[0].alias("trend_id"),
        col("geo").alias("country"),
        col("date_reported").cast(DateType()).alias("date_reported"),
        split_and_cast_as_array("screenshot", r"\s*,\s*").alias("screenshot_media_links"),
        col("link").alias("artifact_link"),
        col("link_s_platform").alias("host_platform"),
        col("online_status").alias("artifact_status"),
        col("comment").alias("addtional_notes"),
        col("item_id").alias("monday_item_id"),
        col("external_trend_id"),
        col("analyst"),
        col("external_artifact_id"),
        col("creation_log").cast(TimestampType()).alias("created_at"),
        col("last_updated").cast(TimestampType()).alias("last_updated"),
        # Extra columns
        col("_rescued_data"),
        col("ingestion_timestamp"),
        col("source_file"),
        col("processing_date"),
        col("file_name"),
    ]
    
    return (
        dlt.readStream(landing_cdc_table_name)
        .select(*select_exprs)
    )
