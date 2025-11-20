from pyspark.sql.types import ArrayType, StringType, StructType, StructField, TimestampType

cleansed_cdc_table_schema = StructType([
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
    # Extra columns
    StructField("_rescued_data", StringType(), True, metadata={"comment": "Rescued data for schema evolution"}),
    StructField("ingestion_timestamp", TimestampType(), True, metadata={"comment": "Timestamp of data ingestion"}),
    StructField("source_file", StringType(), True, metadata={"comment": "Source file path"}),
    StructField("processing_date", TimestampType(), True, metadata={"comment": "Date of processing"}),
    StructField("file_name", StringType(), True, metadata={"comment": "Name of the ingested file"}),
])


import dlt
from pyspark.sql.functions import col, split, trim, expr
import sys
sys.path.append("..")
from config import (
    cleansed_cdc_table_name,
    landing_cdc_table_name
)

dlt.create_streaming_table(
    name=cleansed_cdc_table_name,
    comment="Cleaned and structured Amazon GenAI trends data with enforced schema - optimized for full-refresh CDC",
    schema=cleansed_cdc_table_schema,
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.tuneFileSizesForRewrites": "true",
        "delta.enableChangeDataFeed": "true"  # Enable CDC feed for downstream processing
    },
    partition_cols=["processing_date"],
    expect_all_or_drop={
        "valid_item_id": "item_id IS NOT NULL AND LENGTH(item_id) > 0",
        "valid_timestamp": "ingestion_timestamp IS NOT NULL",
        "valid_source": "source_file IS NOT NULL",
        "no_rescued_data": "_rescued_data IS NULL"
    },
)

def split_and_cast_as_array(field, pattern):
    return (split(trim(col(field)), pattern).cast(ArrayType(StringType()))).alias(field)

@dlt.append_flow(
    target=cleansed_cdc_table_name,
    comment="Cleaned and structured Amazon GenAI trends data with enforced schema flow - optimized for full-refresh CDC",
)
def cleansed_cdc_table_flow():
    """
    Cleansed layer: Create CDC table optimized for full-refresh data ingestion.
    Processes all data from landing table with proper data type conversions and array splitting.
    Optimized for scenarios where each batch contains the complete dataset.
    This table serves as the source for downstream CDC operations using dlt.create_auto_cdc_flow.
    """

    # Define the schema for subitems (as per the structure in the original transform)
    subitems_schema = ArrayType(
        StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("color_mknpc2jw", StringType(), True),
            StructField("text_mknrh63d", StringType(), True),
            StructField("status", StringType(), True),
            StructField("file_mkpcr802", StringType(), True),
            StructField("text_mknwd42e", StringType(), True),
            StructField("long_text_mknp2rqj", StringType(), True),
            StructField("long_text_mknps02y", StringType(), True),
        ])
    )

    # Build select expressions for all columns, applying split/cast inline for relevant fields
    select_exprs = [
        col("id").alias("item_id"),
        col("name").alias("item_name"),

        # Parse the subitems string column as an array of structs, then transform as before
        # Use from_json directly with the schema object
        expr(
            """
            transform(
                from_json(subitems, '{schema}'),
                x -> named_struct(
                    'id', x.id,
                    'name', x.name,
                    'type', x.color_mknpc2jw,
                    'description', x.text_mknrh63d,
                    'status', x.status,
                    'screenshots', x.file_mkpcr802,
                    'summary_update', x.text_mknwd42e,
                    'long_text_1', x.long_text_mknp2rqj,
                    'long_text_2', x.long_text_mknps02y
                )
            )
            """.format(schema=subitems_schema.json())
        ).alias("subitems"),
        # Split and cast 'geo' as array (fix for downstream explode)
        split_and_cast_as_array("geo", r"\s*,\s*"),
        col("trend_id").alias("trend_id"),
        col("researcher").alias("researcher"),
        col("reviewer").alias("reviewer"),
        col("trend_type").alias("trend_type"),
        col("trend_sub_category").alias("trend_sub_category"),
        # Split and cast 'language', 'key_modality', 'sample_source', 'screenshots', 'platform_presence', 'internal_link'
        split_and_cast_as_array("language", r"\s*,\s*"),
        split_and_cast_as_array("key_modality", r"\s*,\s*"),
        col("description").alias("description"),
        col("additional_context").alias("additional_context"),
        split_and_cast_as_array("sample_source", r"\s*,\s*"),
        split_and_cast_as_array("screenshots", r"\s*,\s*"),
        split_and_cast_as_array("platform_presence", r"\s*,\s*"),
        col("msm_date").alias("msm_date"),
        col("related_entity").alias("related_entity"),
        col("msm_link").alias("msm_link"),
        col("engagement_at_detection").alias("engagement_at_detection"),
        col("internal_anaylst_notes").alias("internal_analyst_notes"),
        col("internal_reviewer_notes").alias("internal_reviewer_notes"),
        col("creation_log").alias("creation_log"),
        col("timeline").alias("timeline"),
        col("detection_source").alias("detection_source"),
        split_and_cast_as_array("internal_link", r"\s*,\s*"),
        col("monday_doc_v2").alias("monday_doc_v2"),
        col("ssh_original_description").alias("ssh_original_description"),
        col("ssh_original_context").alias("ssh_original_context"),
        # Split and cast 'ssh_language' by space
        split_and_cast_as_array("ssh_language", r"\s+"),
        col("summarize_updates").alias("summarize_updates"),
        col("etd"),
        col("etd_draft_link_id"),
        col("etd_link_id"),
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
