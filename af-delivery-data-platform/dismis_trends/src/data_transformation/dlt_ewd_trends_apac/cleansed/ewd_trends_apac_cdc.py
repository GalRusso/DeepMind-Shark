from pyspark.sql.types import ArrayType, StringType, StructType, StructField, TimestampType, DateType

cleansed_cdc_table_schema = StructType([
    StructField("trend_id", StringType(), True, metadata={"comment": "Trend ID"}),
    StructField("external_trend_id", StringType(), True, metadata={"comment": "External Trend ID from client's database"}),
    StructField("internal_date_reported", DateType(), True, metadata={"comment": "The internal date reported, used to be the main reported date with the client before we shift to the new TRIX"}),
    StructField("country", StringType(), True, metadata={"comment": "The country which we identified the trend was from"}),
    StructField("date_published_link", DateType(), True, metadata={"comment": "Publish date of the example source link"}),
    StructField("source_platform", StringType(), True, metadata={"comment": "Source of the platform"}),
    StructField("example_links", ArrayType(StringType()), True, metadata={"comment": "Example links to the source"}),
    StructField("screenshot_media_links", ArrayType(StringType()), True, metadata={"comment": "Actual media link of the screenshots (link to monday media)"}),
    StructField("screenshot_folder_link", StringType(), True, metadata={"comment": "Google Drive folder link to the screenshots"}),
    StructField("primary_language", StringType(), True, metadata={"comment": "Primary language that has been used for this trend"}),
    StructField("trend_presentation_method", StringType(), True, metadata={"comment": "Means of narrative representation (by video, image or own narrative voice)"}),
    StructField("trend_name", StringType(), True, metadata={"comment": "Name of the trend"}),
    StructField("trend_description", StringType(), True, metadata={"comment": "Short description what the trend is about"}),
    StructField("additional_information", StringType(), True, metadata={"comment": "Additional information to help understand the trend better, also contains secondary (tertiary) Countries, Language"}),
    StructField("platform_spread", ArrayType(StringType()), True, metadata={"comment": "List of platforms the trend is spreading (list of values)"}),
    StructField("related_trend_ids", ArrayType(StringType()), True, metadata={"comment": "IDs of past trends with similar narratives or had mentioned similar entities (multiple trends)"}),
    StructField("keywords", ArrayType(StringType()), True, metadata={"comment": "Keywords extracted from the trend in the narratives that give the content, topic of the trend"}),
    StructField("potential_products_impacted", ArrayType(StringType()), True, metadata={"comment": "Potential Google products might get involved in the trend"}),
    StructField("reach_estimation", StringType(), True, metadata={"comment": "Estimation of trend reach (at point of estimation)"}),
    StructField("risk_estimation", StringType(), True, metadata={"comment": "Estimation of trend risk"}),
    StructField("msm_date", DateType(), True, metadata={"comment": "Date of mainstream media reporting on trend (YYYY-MM-DD)"}),
    StructField("msm_link", StringType(), True, metadata={"comment": "Mainstream media link"}),
    StructField("analyst", StringType(), True, metadata={"comment": "Analyst who labels the trend"}),
    StructField("internal_content_tags", ArrayType(StringType()), True, metadata={"comment": "Internal ActiveFence tag using for grouping data"}),
    StructField("client_feedback", StringType(), True, metadata={"comment": "Clients' feedback on labelled data"}),
    StructField("monday_item_id", StringType(), True, metadata={"comment": "Monday Board Item ID"}),
    StructField("client_feedback_date", DateType(), True, metadata={"comment": "Date of client feedback (YYYY-MM-DD)"}),
    StructField("trend_type", StringType(), True, metadata={"comment": "Topic/Genre/Type of the trend"}),
    StructField("content_tags", ArrayType(StringType()), True, metadata={"comment": "Hashtag/ identifier of trend on social media"}),
    StructField("risk_type", StringType(), True, metadata={"comment": "Type of risk/ abuse involved in the link"}),
    StructField("external_date_reported", DateType(), True, metadata={"comment": "Date of client feedback on labeled data (YYYY-MM-DD)"}),
    StructField("created_at", TimestampType(), True, metadata={"comment": "The time that the record was created (UTC)"}),
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
    comment="Cleaned and structured EWD APAC trends data with enforced schema",
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
        "valid_trend_name": "trend_name IS NOT NULL",
        # TODO: Move this to expect_all_or_fail when the trend_presentation_method is fixed
        "valid_trend_presentation_method": "trend_presentation_method IN ('Image', 'Video', 'Narrative', 'Audio', 'Article', 'Warning')",
        "valid_trend_id_prefix": "trend_id LIKE 'APAC-%' or trend_id LIKE 'IN-%'",
        "valid_risk_estimation": "risk_estimation IN ('Low', 'Medium', 'High')",
        "valid_risk_type": "risk_type IN ('Geo Politics', 'Social Unrest', 'Health Misinfo', 'Vulnerable Groups', 'Political Misinfo', 'Violence/Extremism/Hate', 'General Misinfo', 'Fraud/Scam/User Data')",
        "valid_trend_type": "trend_type IN ('Politics', 'Crisis', 'Natural Disaster', 'Social Unrest', 'War/Conflict', 'Figure/Actor/Group/Channel', 'Immigration', 'Health', 'Climate')",
        "primary_language_has_one_value": "primary_language NOT LIKE '%,%'",
        "example_links_not_empty": "size(example_links) >= 1"
    },
)

def split_and_cast_as_array(field, pattern):
    return (split(trim(col(field)), pattern).cast(ArrayType(StringType()))).alias(field)

@dlt.append_flow(
    target=cleansed_cdc_table_name,
    comment="Cleaned and structured EWD APAC trends data with enforced schema flow",
)
def landing_to_cleansed_cdc_flow():
    """
    Cleansed layer: Create CDC table optimized for full-refresh data ingestion.
    Processes all data from landing table with proper data type conversions and array splitting.
    Optimized for scenarios where each batch contains the complete dataset.
    This table serves as the source for downstream CDC operations using dlt.create_auto_cdc_flow.
    """
    select_exprs = [
        col("name").alias("trend_id"),
        col("external_trend_id"),
        col("internal_date_reported").cast(DateType()).alias("internal_date_reported"),
        col("geo").alias("country"),
        col("date_published_link").cast(DateType()).alias("date_published_link"),
        col("source_platform"),
        split_and_cast_as_array("example_link", r"[\s,;]+").alias("example_links"),
        split_and_cast_as_array("screenshot", r"\s*,\s*").alias("screenshot_media_links"),
        col("screenshot_link_folder").alias("screenshot_folder_link"),
        col("language_spread_primary").alias("primary_language"),
        col("trend_medium").alias("trend_presentation_method"),
        col("trend_name"),
        col("trend_description"),
        col("additional_notes").alias("additional_information"),
        split_and_cast_as_array("platform_spread", r"\s*,\s*").alias("platform_spread"),
        split_and_cast_as_array("related_trends", r"\s*,\s*").alias("related_trend_ids"),
        split_and_cast_as_array("keywords", r"\s*,\s*").alias("keywords"),
        split_and_cast_as_array("potential_products_impacted", r"\s*,\s*").alias("potential_products_impacted"),
        col("reach_estimation"),
        col("risk_estimation"),
        col("msm_date").cast(DateType()).alias("msm_date"),
        col("msm_link"),
        col("analyst"),
        col("internal_content_tag").alias("internal_content_tags"),
        col("client_feedback"),
        col("item_id").alias("monday_item_id"),
        col("automated_client_feedback_date").cast(DateType()).alias("client_feedback_date"),
        col("trend_type"),
        split_and_cast_as_array("content_tag", r"\s*,\s*").alias("content_tags"),
        col("risk_type"),
        col("external_date_reported").cast(DateType()).alias("external_date_reported"),
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
