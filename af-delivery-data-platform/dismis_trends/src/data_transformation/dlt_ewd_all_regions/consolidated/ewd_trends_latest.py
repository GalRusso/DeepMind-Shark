from pyspark.sql.types import ArrayType, StringType, StructType, StructField, TimestampType, DateType

# Schema for the latest snapshot table - consolidated for all regions
table_schema = StructType([
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
    regional_trends_latest_tables,
    trends_latest_table_name
)

@dlt.table(
    name=trends_latest_table_name,
    comment="Latest snapshot of each EWD Trend Items across all regions (US, EMEA, APAC)",
    schema=table_schema,
    table_properties={
        "quality": "silver",
        "delta.enableRowTracking": "true"
    },
    partition_cols=["region", "created_at"]
)
def ewd_trends_all_regions_latest():
    """
    Consolidate latest trends data from all regional latest tables.
    Reads from existing regional latest tables and adds region identification.
    """
    # Read from each regional latest table and add region identifier
    us_data = (
        dlt.read(regional_trends_latest_tables["us"])
        .withColumn("region", lit("US"))
    )
    
    emea_data = (
        dlt.read(regional_trends_latest_tables["emea"])
        .withColumn("region", lit("EMEA"))
    )
    
    apac_data = (
        dlt.read(regional_trends_latest_tables["apac"])
        .withColumn("region", lit("APAC"))
    )
    
    # Union all regional data
    return us_data.union(emea_data).union(apac_data)