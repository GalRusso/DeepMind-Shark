from pyspark.sql.types import ArrayType, StringType, StructField, StructType, TimestampType

# Schema for the latest snapshot table - same as cleansed CDC but optimized for latest records
cleansed_latest_table_schema = StructType([
    StructField("item_id", StringType(), True, metadata={"comment": "Monday item id"}),
    StructField("item_name", StringType(), True, metadata={"comment": "Monday.com item name"}),
    StructField("subitems", ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("description", StringType(), True),
        StructField("status", StringType(), True),
        StructField("screenshots", StringType(), True),
        StructField("summary_update", StringType(), True),
        StructField("long_text_1", StringType(), True),
        StructField("long_text_2", StringType(), True),
    ])), True, metadata={"comment": "Subitems information"}),
    StructField("geo", ArrayType(StringType()), True, metadata={"comment": "Geographic locations"}),
    StructField("trend_id", StringType(), True, metadata={"comment": "Trend identifier"}),
    StructField("researcher", StringType(), True, metadata={"comment": "Researcher name"}),
    StructField("reviewer", StringType(), True, metadata={"comment": "Reviewer name"}),
    StructField("trend_type", StringType(), True, metadata={"comment": "Type of trend"}),
    StructField("trend_sub_category", StringType(), True, metadata={"comment": "Trend sub-category"}),
    StructField("language", ArrayType(StringType()), True, metadata={"comment": "Language information"}),
    StructField("key_modality", ArrayType(StringType()), True, metadata={"comment": "Key modality"}),
    StructField("description", StringType(), True, metadata={"comment": "Trend description"}),
    StructField("additional_context", StringType(), True, metadata={"comment": "Additional context"}),
    StructField("sample_source", ArrayType(StringType()), True, metadata={"comment": "Sample source"}),
    StructField("screenshots", ArrayType(StringType()), True, metadata={"comment": "Screenshots"}),
    StructField("platform_presence", ArrayType(StringType()), True, metadata={"comment": "Platform presence"}),
    StructField("msm_date", StringType(), True, metadata={"comment": "Mainstream media date"}),
    StructField("related_entity", StringType(), True, metadata={"comment": "Related entity"}),
    StructField("msm_link", StringType(), True, metadata={"comment": "Mainstream media link"}),
    StructField("engagement_at_detection", StringType(), True, metadata={"comment": "Engagement at detection"}),
    StructField("internal_analyst_notes", StringType(), True, metadata={"comment": "Internal analyst notes"}),
    StructField("internal_reviewer_notes", StringType(), True, metadata={"comment": "Internal reviewer notes"}),
    StructField("creation_log", StringType(), True, metadata={"comment": "Creation log"}),
    StructField("timeline", StringType(), True, metadata={"comment": "Timeline"}),
    StructField("detection_source", StringType(), True, metadata={"comment": "Detection source"}),
    StructField("internal_link", ArrayType(StringType()), True, metadata={"comment": "Internal link"}),
    StructField("monday_doc_v2", StringType(), True, metadata={"comment": "Monday document v2"}),
    StructField("ssh_original_description", StringType(), True, metadata={"comment": "SSH original description"}),
    StructField("ssh_original_context", StringType(), True, metadata={"comment": "SSH original context"}),
    StructField("ssh_language", ArrayType(StringType()), True, metadata={"comment": "SSH language"}),
    StructField("summarize_updates", StringType(), True, metadata={"comment": "Summarize updates"}),
    StructField("etd", StringType(), True, metadata={"comment": "ETD"}),
    StructField("etd_draft_link_id", StringType(), True, metadata={"comment": "ETD draft link ID"}),
    StructField("etd_link_id", StringType(), True, metadata={"comment": "ETD link ID"}),
    # Metadata columns for latest snapshot
    StructField("ingestion_timestamp", TimestampType(), True, metadata={"comment": "Timestamp of data ingestion"}),
    StructField("source_file", StringType(), True, metadata={"comment": "Source file path"}),
    StructField("processing_date", TimestampType(), True, metadata={"comment": "Date of processing"}),
    StructField("file_name", StringType(), True, metadata={"comment": "Name of the ingested file"}),
])

import dlt
from pyspark.sql.functions import col
import sys
sys.path.append("..")
from config import (
    cleansed_latest_table_name,
    cleansed_cdc_table_name
)

# Create the latest snapshot table with optimized configuration
dlt.create_streaming_table(
    name=cleansed_latest_table_name,
    schema=cleansed_latest_table_schema,
    comment="Latest snapshot of each Amazon GenAI trend item - using DLT AUTO CDC best practices",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.tuneFileSizesForRewrites": "true"
    },
    partition_cols=["processing_date"],
    expect_all_or_drop={
        "valid_item_id": "item_id IS NOT NULL AND LENGTH(item_id) > 0",
        "valid_timestamp": "ingestion_timestamp IS NOT NULL",
        "valid_source": "source_file IS NOT NULL"
    }
)

# Use DLT AUTO CDC for efficient change data capture - this is the recommended best practice
dlt.create_auto_cdc_flow(
    target=cleansed_latest_table_name,
    source=cleansed_cdc_table_name,
    keys=["item_id"],
    sequence_by=col("ingestion_timestamp"),
    apply_as_deletes=None,  # No delete operations in this use case
    except_column_list=["_rescued_data"],  # Include all columns
    stored_as_scd_type=1  # SCD Type 1 - keep only latest version
)
