import dlt
from pyspark.sql.functions import col, when, trim, upper, to_date
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType, TimestampType
from config import landing_cdc_table_name, cleansed_cdc_table_name

schema = StructType([
    StructField("sent_to_clients", StringType(), True, metadata={"comment": "Client name this prompt was sent to"}),
    StructField("date_delivered", DateType(), True, metadata={"comment": "Date this prompt was sent to the client (YYYY-MM-DD)"}),
    StructField("prompt_id", StringType(), True, metadata={"comment": "Unique identifier of this prompt"}),
    StructField("abuse_area_name", StringType(), True, metadata={"comment": "Violation name, based on client's policy"}),
    StructField("sub_abuse_area_name", StringType(), True, metadata={"comment": "Specific topic within the violation"}),
    StructField("internal_abuse_area_name", StringType(), True, metadata={"comment": "Violation topic based on our INTERNAL policy"}),
    StructField("language", StringType(), True, metadata={"comment": "The language this prompt is written in"}),
    StructField("translated_to", StringType(), True, metadata={"comment": "The language this prompt was translated to"}),
    StructField("translated_prompt", StringType(), True, metadata={"comment": "Non-english prompt's translation to english"}),
    StructField("text_prompt", StringType(), True, metadata={"comment": "The prompt itself"}),
    StructField("project", StringType(), True, metadata={"comment": "Exercise/task given by the client"}),
    StructField("prompt_type", StringType(), True, metadata={"comment": "For product -- always Safety"}),
    StructField("modality_support", StringType(), True, metadata={"comment": "Modality this prompt was written for"}),
    StructField("is_generated", BooleanType(), True, metadata={"comment": "A flag determine if the prompt content is automatically generated or manually created (standardized)"}),
    StructField("industry", StringType(), True, metadata={"comment": "For product -- always Foundation Model"}),
    StructField("date_uploaded", DateType(), True, metadata={"comment": "Date this prompt was added into the data source (YYYY-MM-DD)"}),

    # Metadata fields added during processing
    StructField("ingestion_timestamp", TimestampType(), True, metadata={"comment": "Timestamp when data was ingested from source"}),
    StructField("source_file", StringType(), True, metadata={"comment": "Source file path for data lineage"}),
    StructField("file_name", StringType(), True, metadata={"comment": "Source file name"}),
    StructField("processing_date", TimestampType(), True, metadata={"comment": "Timestamp when data was processed in this layer"}),
])


dlt.create_streaming_table(
    name=cleansed_cdc_table_name,
    comment="Cleansed Bard data with standardized formats and data quality checks",
    schema=schema,
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    partition_cols=["processing_date"],
    expect_all_or_fail={
        "valid_date_delivered_not_empty": "date_delivered IS NOT NULL AND LENGTH(date_delivered) > 0",
        "valid_date_uploaded_not_empty": "date_uploaded IS NOT NULL AND LENGTH(date_uploaded) > 0",
        "valid_prompt_id_not_empty": "prompt_id IS NOT NULL AND LENGTH(prompt_id) > 0",
        "valid_text_prompt_not_empty": "text_prompt IS NOT NULL AND LENGTH(text_prompt) > 0",
    }
)

@dlt.append_flow(
    target=cleansed_cdc_table_name,
    comment="Cleansed Bard data with standardized formats and data quality checks flow",
)
def landing_to_cleansed_cdc_flow():
    """
    Silver layer: Clean and standardize Bard data from landing layer.
    Applies data quality rules, standardizes formats, and handles missing values.
    Converts date strings to proper date formats and standardizes text fields.
    """
    
    return (
        dlt.read_stream(landing_cdc_table_name)
        .select(
            trim(col("sent_to_clients")).alias("sent_to_clients"),
            when(col("date_delivered").isNotNull() & (col("date_delivered") != ""),
                 to_date(col("date_delivered"), "MM/dd/yyyy")).alias("date_delivered"),
            trim(col("prompt_id")).alias("prompt_id"),
            trim(col("abuse_area_name")).alias("abuse_area_name"),
            trim(col("sub_abuse_area_name")).alias("sub_abuse_area_name"),
            trim(col("internal_abuse_area_name")).alias("internal_abuse_area_name"),
            trim(col("language")).alias("language"),
            trim(col("translated_to")).alias("translated_to"),
            trim(col("translated_prompt")).alias("translated_prompt"),
            trim(col("text_prompt")).alias("text_prompt"),
            trim(col("project")).alias("project"),
            trim(col("prompt_type")).alias("prompt_type"),
            trim(col("modality_support")).alias("modality_support"),
            when(upper(trim(col("is_generated"))).isin(["TRUE", "1", "YES", "Y"]), True)
                .when(upper(trim(col("is_generated"))).isin(["FALSE", "0", "NO", "N"]), False)
                .otherwise(None).alias("is_generated"),
            trim(col("industry")).alias("industry"),
            when(col("date_uploaded").isNotNull() & (col("date_uploaded") != ""),
                 to_date(col("date_uploaded"), "MM/dd/yyyy")).alias("date_uploaded"),
            col("ingestion_timestamp"),
            col("source_file"),
            col("processing_date"),
            col("file_name"),
        )
    )