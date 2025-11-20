from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType, TimestampType

# Schema for the latest snapshot table - based on CDC table schema
cleansed_latest_table_schema = StructType([
    StructField("sent_to_clients", StringType(), True, metadata={"comment": "Client name this prompt was sent to"}),
    StructField("date_delivered", DateType(), True, metadata={"comment": "Date this prompt was sent to the client (YYYY-MM-DD)"}),
    StructField("prompt_id", StringType(), True, metadata={"comment": "Unique identifier of this prompt"}),
    StructField("abuse_area_name", StringType(), True, metadata={"comment": "Violation name, based on client's policy"}),
    StructField("sub_abuse_area_name", StringType(), True, metadata={"comment": "Specific topic within the violation"}),
    StructField("internal_abuse_area_name", StringType(), True, metadata={"comment": "Violation topic based on our INTERNAL policy"}),
    StructField("text_prompt", StringType(), True, metadata={"comment": "The prompt itself"}),
    StructField("project", StringType(), True, metadata={"comment": "Exercise/task given by the client"}),
    StructField("prompt_type", StringType(), True, metadata={"comment": "For product -- always Safety"}),
    StructField("modality_support", StringType(), True, metadata={"comment": "Modality this prompt was written for"}),
    StructField("is_generated", BooleanType(), True, metadata={"comment": "A flag determine if the prompt content is automatically generated or manually created (standardized)"}),
    StructField("industry", StringType(), True, metadata={"comment": "For product -- always Foundation Model"}),
    StructField("response_label", StringType(), True, metadata={"comment": "when comparing models - whether the response the model provided is safe or unsafe (violative of the policy)"}),
    StructField("target_special_instructions", StringType(), True, metadata={"comment": "System prompt"}),
    StructField("date_uploaded", DateType(), True, metadata={"comment": "Date this prompt was added into the data source (YYYY-MM-DD)"}),
    # Metadata fields
    StructField("ingestion_timestamp", TimestampType(), True, metadata={"comment": "Timestamp when data was ingested from source"}),
    StructField("source_file", StringType(), True, metadata={"comment": "Source file path for data lineage"}),
    StructField("file_name", StringType(), True, metadata={"comment": "Source file name"}),
    StructField("processing_date", TimestampType(), True, metadata={"comment": "Timestamp when data was processed in this layer"}),
])

import dlt
from pyspark.sql.functions import col
from config import cleansed_cdc_table_name, cleansed_latest_table_name

# Create the latest snapshot table with updated configuration
dlt.create_streaming_table(
    name=cleansed_latest_table_name,
    schema=cleansed_latest_table_schema,
    comment="Latest snapshot of each AGI prompt",
    table_properties={
        "quality": "silver",
        "delta.enableRowTracking": "true"
    }
)

# Use DLT AUTO CDC for efficient change data capture
dlt.create_auto_cdc_flow(
    target=cleansed_latest_table_name,
    source=cleansed_cdc_table_name,
    keys=["prompt_id"],
    sequence_by=col("ingestion_timestamp"),
    apply_as_deletes=None,  # No delete operations in this use case
    stored_as_scd_type=1  # SCD Type 1 - keep only latest version
)
