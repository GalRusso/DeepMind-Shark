import dlt
from pyspark.sql.functions import col, current_timestamp, regexp_extract, to_timestamp
from config import SOURCE_PATH, FILE_NAME_PATTERN, landing_cdc_table_name


dlt.create_streaming_table(
    name=landing_cdc_table_name,
    comment="Raw EWD US copies data ingested from Monday.com board JSON exports",
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    expect_all_or_drop={
        "valid_send_not_empty": "sent IS NOT NULL"
    }
)


@dlt.append_flow(
    target=landing_cdc_table_name,
    comment="Raw EWD US copies data ingested from Monday.com board JSON exports flow",
)
def s3_to_landing_flow():
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
        .option("cloudFiles.inferColumnTypes", "true") # Infer column types from the data
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Handle schema evolution gracefully
        .option("multiLine", "true") # Handle multi-line JSON
        .option("inferSchema", "true") # Infer schema from the data
        .option("pathGlobFilter", FILE_NAME_PATTERN)
        .load(SOURCE_PATH)
    )

    return (
        df
        .withColumn(
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
