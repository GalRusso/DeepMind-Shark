import dlt
from pyspark.sql.functions import col, current_timestamp, regexp_extract, to_timestamp
from config import SOURCE_PATH, FILE_NAME_PATTERN, landing_cdc_table_name


dlt.create_streaming_table(
    name=landing_cdc_table_name,
    comment="Raw Cohere data ingested from Google Sheets CSV exports",
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    expect_all_or_drop={
        "valid_prompt_id_not_empty": "prompt_id IS NOT NULL AND LENGTH(prompt_id) > 0",
        "valid_text_prompt_not_empty": "text_prompt IS NOT NULL AND LENGTH(text_prompt) > 0",
        "valid_date_uploaded_not_empty": "date_uploaded IS NOT NULL AND LENGTH(date_uploaded) > 0",
        "valid_date_delivered_not_empty": "date_delivered IS NOT NULL AND LENGTH(date_delivered) > 0",
        
    }
)


@dlt.append_flow(
    target=landing_cdc_table_name,
    comment="Raw Cohere data ingested from Google Sheets CSV exports flow",
)
def s3_to_landing_flow():
    """
    Bronze layer: Ingest raw CSV data from S3 using Autoloader.
    Optimized for incremental data ingestion where each batch contains filtered data.
    Assumes each CSV file contains Cohere prompt data with proper headers.
    Adds metadata columns for data lineage and processing tracking.
    """
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiline", "true")
        .option("escape", "\"")
        .option("quote", "\"")
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
            "file_name", regexp_extract(col("source_file"), r"([^/]+)\.csv$", 1)
        )
    )
