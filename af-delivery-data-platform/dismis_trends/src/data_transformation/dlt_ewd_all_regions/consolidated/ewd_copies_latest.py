from pyspark.sql.types import ArrayType, StringType, StructType, StructField, TimestampType, DateType

# Schema for the latest snapshot table - consolidated for all regions
table_schema = StructType([
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
    # Region identification
    StructField("region", StringType(), True, metadata={"comment": "Region identifier (US, EMEA, APAC)"}),
    # Extra columns
    StructField("ingestion_timestamp", TimestampType(), True, metadata={"comment": "Timestamp of data ingestion"}),
    StructField("source_file", StringType(), True, metadata={"comment": "Source file path"}),
    StructField("processing_date", TimestampType(), True, metadata={"comment": "Date of processing"}),
    StructField("file_name", StringType(), True, metadata={"comment": "Name of the ingested file"}),
])

import dlt
from pyspark.sql.functions import lit
from config import (
    regional_copies_latest_tables,
    copies_latest_table_name
)

@dlt.table(
    name=copies_latest_table_name,
    comment="Latest snapshot of each EWD Copies Items across all regions (US, EMEA, APAC). These copies are the artifacts that related to the trends.",
    schema=table_schema,
    table_properties={
        "quality": "silver",
        "delta.enableRowTracking": "true"
    },
    partition_cols=["region", "created_at"]
)
def ewd_copies_all_regions_latest():
    """
    Consolidate latest copies data from all regional latest tables.
    Reads from existing regional latest tables and adds region identification.
    """
    # Read from each regional latest table and add region identifier
    us_data = (
        dlt.read(regional_copies_latest_tables["us"])
        .withColumn("region", lit("US"))
    )
    
    emea_data = (
        dlt.read(regional_copies_latest_tables["emea"])
        .withColumn("region", lit("EMEA"))
    )
    
    apac_data = (
        dlt.read(regional_copies_latest_tables["apac"])
        .withColumn("region", lit("APAC"))
    )
    
    # Union all regional data
    return us_data.union(emea_data).union(apac_data)