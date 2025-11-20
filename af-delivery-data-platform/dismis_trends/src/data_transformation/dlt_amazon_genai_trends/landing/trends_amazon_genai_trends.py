from pyspark.sql.types import StructType, StructField, StringType, TimestampType

landing_table_schema = StructType([
    StructField("name", StringType(), True, metadata={"comment": "Monday.com item name"}),
    StructField("id", StringType(), True, metadata={"comment": "Monday ID"}),
    StructField("subitems", StringType(), True, metadata={"comment": "Subitems information"}),
    StructField("subitems_status", StringType(), True, metadata={"comment": "Subitems status"}),
    StructField("geo", StringType(), True, metadata={"comment": "Geographic location"}),
    StructField("trend_id", StringType(), True, metadata={"comment": "Trend identifier"}),
    StructField("analyst_poc", StringType(), True, metadata={"comment": "Analyst point of contact"}),
    StructField("researcher", StringType(), True, metadata={"comment": "Researcher name"}),
    StructField("reviewer", StringType(), True, metadata={"comment": "Reviewer name"}),
    StructField("trend_type", StringType(), True, metadata={"comment": "Type of trend"}),
    StructField("trend_sub_category", StringType(), True, metadata={"comment": "Trend sub-category"}),
    StructField("language", StringType(), True, metadata={"comment": "Language information"}),
    StructField("key_modality", StringType(), True, metadata={"comment": "Key modality"}),
    StructField("description", StringType(), True, metadata={"comment": "Trend description"}),
    StructField("additional_context", StringType(), True, metadata={"comment": "Additional context"}),
    StructField("sample_source", StringType(), True, metadata={"comment": "Sample source"}),
    StructField("screenshots", StringType(), True, metadata={"comment": "Screenshots"}),
    StructField("platform_presence", StringType(), True, metadata={"comment": "Platform presence"}),
    StructField("msm_date", StringType(), True, metadata={"comment": "Mainstream media date"}),
    StructField("related_entity", StringType(), True, metadata={"comment": "Related entity"}),
    StructField("msm_link", StringType(), True, metadata={"comment": "Mainstream media link"}),
    StructField("engagement_at_detection", StringType(), True, metadata={"comment": "Engagement at detection"}),
    StructField("internal_anaylst_notes", StringType(), True, metadata={"comment": "Internal analyst notes"}),
    StructField("internal_reviewer_notes", StringType(), True, metadata={"comment": "Internal reviewer notes"}),
    StructField("status_move_to_researcher", StringType(), True, metadata={"comment": "Status move to researcher"}),
    StructField("status_researcher", StringType(), True, metadata={"comment": "Status researcher"}),
    StructField("status_review", StringType(), True, metadata={"comment": "Status review"}),
    StructField("pm_indicator_review", StringType(), True, metadata={"comment": "PM indicator review"}),
    StructField("creation_log", StringType(), True, metadata={"comment": "Creation log"}),
    StructField("timeline", StringType(), True, metadata={"comment": "Timeline"}),
    StructField("detection_source", StringType(), True, metadata={"comment": "Detection source"}),
    StructField("internal_link", StringType(), True, metadata={"comment": "Internal link"}),
    StructField("monday_doc_v2", StringType(), True, metadata={"comment": "Monday document v2"}),
    StructField("ssh_original_description", StringType(), True, metadata={"comment": "SSH original description"}),
    StructField("ssh_original_context", StringType(), True, metadata={"comment": "SSH original context"}),
    StructField("ssh_language", StringType(), True, metadata={"comment": "SSH language"}),
    StructField("summarize_updates", StringType(), True, metadata={"comment": "Summarize updates"}),
    StructField("etd", StringType(), True, metadata={"comment": "ETD"}),
    StructField("etd_draft_link_id", StringType(), True, metadata={"comment": "ETD draft link ID"}),
    StructField("etd_link_id", StringType(), True, metadata={"comment": "ETD link ID"}),
    
    # Extra metadata columns
    StructField("_rescued_data", StringType(), True, metadata={"comment": "Rescued data for schema evolution"}),
    StructField("ingestion_timestamp", TimestampType(), True, metadata={"comment": "Timestamp of data ingestion"}),
    StructField("source_file", StringType(), True, metadata={"comment": "Source file path"}),
    StructField("file_name", StringType(), True, metadata={"comment": "Name of the ingested file"}),
    StructField("processing_date", TimestampType(), True, metadata={"comment": "Date of processing"}),
])

import dlt
from pyspark.sql.functions import col, current_timestamp, regexp_extract, to_timestamp
import sys
sys.path.append("..")
from config import (
    landing_cdc_table_name,
    SOURCE_PATH,
    FILE_NAME_PATTERN
)

# Bronze Layer - Raw data ingestion with Autoloader (optimized for full-refresh)

dlt.create_streaming_table(
    name=landing_cdc_table_name,
    comment="Raw Amazon GenAI trends data ingested from Monday.com board JSON exports - optimized for full-refresh",
    schema=landing_table_schema,
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.tuneFileSizesForRewrites": "true"
    },
    partition_cols=["ingestion_timestamp"]
)


@dlt.append_flow(
    target=landing_cdc_table_name,
    comment="Raw Amazon GenAI trends data ingested from Monday.com board JSON exports flow - optimized for full-refresh",
)
def landing_table_flow():
    """
    Bronze layer: Ingest raw JSON data from S3 using Autoloader.
    Optimized for full-refresh data ingestion where each batch contains the complete dataset.
    Assumes each JSON file is an array of objects.
    Adds metadata columns for data lineage and processing tracking.
    Renames columns with invalid characters to be Delta-compliant.
    """
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # Handle schema evolution gracefully
        .option("multiLine", "true")
        .option("inferSchema", "true")
        .option("pathGlobFilter", FILE_NAME_PATTERN)
        .load(SOURCE_PATH)
    )

    return (
        df.withColumn(
            "ingestion_timestamp",
            to_timestamp(
                regexp_extract(col("_metadata.file_path"), r"(\d{8}_\d{6})\.[^/]+$", 1),
                "yyyyMMdd_HHmmss",
            ),
        )
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("processing_date", current_timestamp())
        .withColumn(
            "file_name", regexp_extract(col("source_file"), r"([^/]+)\.json$", 1)
        )
    )
