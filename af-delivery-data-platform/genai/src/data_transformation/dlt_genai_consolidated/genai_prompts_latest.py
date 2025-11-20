from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType, TimestampType

# Unified schema for consolidated genai prompts - includes all fields from all modules
consolidated_table_schema = StructType([
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
    StructField("target_type", StringType(), True, metadata={"comment": "Internal modification that was made to create the prompt (example: Distance between words)"}),
    StructField("project", StringType(), True, metadata={"comment": "Exercise/task given by the client"}),
    StructField("prompt_type", StringType(), True, metadata={"comment": "For product -- always Safety"}),
    StructField("modality_support", StringType(), True, metadata={"comment": "Modality this prompt was written for"}),
    StructField("is_generated", BooleanType(), True, metadata={"comment": "A flag determine if the prompt content is automatically generated or manually created (standardized)"}),
    StructField("industry", StringType(), True, metadata={"comment": "For product -- always Foundation Model"}),
    StructField("response_label", StringType(), True, metadata={"comment": "when comparing models - whether the response the model provided is safe or unsafe (violative of the policy)"}),
    StructField("target_special_instructions", StringType(), True, metadata={"comment": "System prompt"}),
    StructField("date_uploaded", DateType(), True, metadata={"comment": "Date this prompt was added into the data source (YYYY-MM-DD)"}),
    # Source identification
    StructField("source_module", StringType(), True, metadata={"comment": "Source genai module (bard, cohere, deepmind, agi)"}),
    # Metadata fields
    StructField("ingestion_timestamp", TimestampType(), True, metadata={"comment": "Timestamp when data was ingested from source"}),
    StructField("source_file", StringType(), True, metadata={"comment": "Source file path for data lineage"}),
    StructField("file_name", StringType(), True, metadata={"comment": "Source file name"}),
    StructField("processing_date", TimestampType(), True, metadata={"comment": "Timestamp when data was processed in this layer"}),
])

import dlt
from pyspark.sql.functions import lit
from config import (
    genai_latest_tables,
    consolidated_latest_table_name
)

@dlt.table(
    name=consolidated_latest_table_name,
    comment="Consolidated latest snapshot of all GenAI prompts from all modules (Bard, Cohere, Deepmind, AGI)",
    schema=consolidated_table_schema,
    table_properties={
        "quality": "gold",
        "delta.enableRowTracking": "true"
    },
    partition_cols=["source_module", "date_uploaded"]
)
def genai_prompts_all_modules_latest():
    """
    Consolidate latest prompts data from all genai modules.
    Reads from existing module latest tables and adds source module identification.
    Handles missing columns by adding null values for schema consistency.
    Ensures all tables have the same column order and data types.
    """
    # Define the standard column order and ensure consistent data types
    standard_columns = [
        "sent_to_clients", "date_delivered", "prompt_id", "abuse_area_name", 
        "sub_abuse_area_name", "internal_abuse_area_name",
        "language", "translated_to", "translated_prompt", "text_prompt", 
        "target_type", "project", "prompt_type", "modality_support", 
        "is_generated", "industry", "response_label", "target_special_instructions", "date_uploaded", 
        "source_module",
        "ingestion_timestamp", "source_file", "file_name", "processing_date"
    ]
    
    # Bard data - missing target_type, ensure proper column order
    bard_data = (
        dlt.read(genai_latest_tables["bard"])
        .withColumn("source_module", lit("bard"))
        .withColumn("target_type", lit(None).cast(StringType()))  # Add missing target_type as null
        .withColumn("response_label", lit(None).cast(StringType()))  # Add missing response_label as null
        .withColumn("target_special_instructions", lit(None).cast(StringType()))  # Add missing target_special_instructions as null
        .select(*standard_columns)  # Ensure consistent column order
    )
    
    # Cohere data - has all fields, ensure proper column order
    cohere_data = (
        dlt.read(genai_latest_tables["cohere"])
        .withColumn("source_module", lit("cohere"))
        .withColumn("target_type", lit(None).cast(StringType()))  # Add missing target_type as null
        .withColumn("response_label", lit(None).cast(StringType()))  # Add missing response_label as null
        .withColumn("target_special_instructions", lit(None).cast(StringType()))  # Add missing target_special_instructions as null
        .select(*standard_columns)  # Ensure consistent column order
    )
    
    # Deepmind data - has all fields, ensure proper column order
    deepmind_data = (
        dlt.read(genai_latest_tables["deepmind"])
        .withColumn("source_module", lit("deepmind"))
        .withColumn("target_type", lit(None).cast(StringType()))  # Add missing target_type as null
        .withColumn("response_label", lit(None).cast(StringType()))  # Add missing response_label as null
        .withColumn("target_special_instructions", lit(None).cast(StringType()))  # Add missing target_special_instructions as null
        .withColumn("translated_to", lit(None).cast(StringType()))  # Add missing translated_to as null
        .withColumn("translated_prompt", lit(None).cast(StringType()))  # Add missing translated_prompt as null
        .select(*standard_columns)  # Ensure consistent column order
    )
    
    agi_data = (
        dlt.read(genai_latest_tables["agi"])
        .withColumn("source_module", lit("agi"))
        .withColumn("language", lit(None).cast(StringType()))  # Add missing language as null
        .withColumn("translated_to", lit(None).cast(StringType()))  # Add missing translated_to as null
        .withColumn("translated_prompt", lit(None).cast(StringType()))  # Add missing translated_prompt as null
        .withColumn("target_type", lit(None).cast(StringType()))  # Add missing target_type as null
        .select(*standard_columns)  # Ensure consistent column order
    )
    
    # Union all module data
    return (bard_data.union(cohere_data).union(deepmind_data).union(agi_data))
