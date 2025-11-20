from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, BooleanType, ArrayType
import dlt
from pyspark.sql.functions import (
    col, when, coalesce, 
    datediff, size, upper, trim,
    lit, broadcast, regexp_extract
)
import sys
sys.path.append('..')
from config import fact_trends_table, trends_latest_table_name, dim_platform_table, dim_trend_type_table, dim_client_feedback_table, dim_mainstream_media_table

# Schema for trends fact table
trends_fact_schema = StructType([
    # Natural Keys
    StructField("trend_id", StringType(), False, metadata={"comment": "Trend ID (natural key)"}),
    StructField("external_trend_id", StringType(), True, metadata={"comment": "External trend ID"}),
    
    # Dimension Keys
    StructField("internal_date_reported_key", DateType(), True, metadata={"comment": "Date when the trend was reported by us internally"}),
    StructField("external_date_reported_key", DateType(), True, metadata={"comment": "Date when our client provided feedback on labeled data"}),
    StructField("date_published_key", DateType(), True, metadata={"comment": "Publish date of example source links"}),
    StructField("msm_date_key", DateType(), True, metadata={"comment": "Date when mainstream media reported on the trend (null if not reported by mainstream media)"}),
    StructField("msm_key", IntegerType(), True, metadata={"comment": "Mainstream media outlet identifier"}),
    StructField("feedback_date_key", DateType(), True, metadata={"comment": "Date when client provided feedback"}),
    StructField("source_platform_key", IntegerType(), True, metadata={"comment": "Platform where the trend was first identified"}),
    StructField("trend_type_key", IntegerType(), True, metadata={"comment": "Trend type and risk categorization identifier"}),
    StructField("feedback_key", IntegerType(), True, metadata={"comment": "Client feedback classification identifier"}),
    
    # Metrics
    StructField("days_to_msm", IntegerType(), True, metadata={"comment": "Days from our internal report date to mainstream media date (positive (>0) if we reported before mainstream media, negative (<0) if we reported after mainstream media, 0 if we reported on the same day as mainstream media, null if no msm coverage)"}),
    StructField("platform_spread_count", IntegerType(), True, metadata={"comment": "Number of platforms trend spread to"}),
    StructField("google_products_impacted_count", IntegerType(), True, metadata={"comment": "Number of Google products potentially impacted"}),
    StructField("related_trends_count", IntegerType(), True, metadata={"comment": "Number of related trends"}),
    StructField("keywords_count", IntegerType(), True, metadata={"comment": "Number of keywords"}),
    StructField("content_tags_count", IntegerType(), True, metadata={"comment": "Number of content tags"}),
    
    # Risk and Reach (original values)
    StructField("risk_estimation", StringType(), True, metadata={"comment": "Original risk estimation text (Example: Low/Medium/High"}),
    StructField("reach_estimation", StringType(), True, metadata={"comment": "Original reach estimation text (Example: 10K - 100K, 100K - 1M, etc.)"}),
    
    # Flags
    StructField("has_msm_coverage", BooleanType(), True, metadata={"comment": "Flag for mainstream media coverage (True if detected by mainstream media)"}),
    StructField("has_client_feedback", BooleanType(), True, metadata={"comment": "Flag for client feedback (True if detected by client feedback)"}),
    StructField("has_google_product_impact", BooleanType(), True, metadata={"comment": "Flag for Google product impact"}),
    StructField("has_multiple_platforms", BooleanType(), True, metadata={"comment": "Flag for multi-platform spread"}),
    StructField("has_screenshots", BooleanType(), True, metadata={"comment": "Flag for screenshot presence (True if screenshots are available)"}),
    
    # Descriptive Attributes
    StructField("trend_name", StringType(), True, metadata={"comment": "Trend name"}),
    StructField("trend_description", StringType(), True, metadata={"comment": "Trend description"}),
    StructField("primary_language", StringType(), True, metadata={"comment": "Primary language used for this trend"}),
    StructField("additional_information", StringType(), True, metadata={"comment": "Additional information including secondary countries and languages"}),
    StructField("example_links", ArrayType(StringType()), True, metadata={"comment": "Example links to the source"}),
    StructField("screenshot_media_links", ArrayType(StringType()), True, metadata={"comment": "Actual media link of the screenshots (link to monday media)"}),
    StructField("screenshot_folder_link", StringType(), True, metadata={"comment": "Google Drive folder link to the screenshots"}),
    StructField("msm_link", StringType(), True, metadata={"comment": "Mainstream media article link"}),
    StructField("analyst", StringType(), True, metadata={"comment": "Analyst name"}),
    StructField("country", StringType(), True, metadata={"comment": "Country name (The country which we identified the trend was from)"}),
    StructField("region", StringType(), True, metadata={"comment": "Region (US, EMEA, APAC)"}),
    
    # Timestamps
    StructField("created_at", TimestampType(), True, metadata={"comment": "Record creation timestamp"}),
    StructField("last_updated", TimestampType(), True, metadata={"comment": "Last update timestamp"})
])

@dlt.table(
    name=fact_trends_table,
    comment="Trends fact table for comprehensive trend analysis",
    schema=trends_fact_schema,
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    },
    partition_cols=["internal_date_reported_key"]
)
def fact_trends():
    """
    Create trends fact table with dimension keys and metrics.
    Central fact table for trend analysis use cases.
    """
    # Read source data
    trends_df = dlt.read(trends_latest_table_name)
    
    # Read dimension tables to get keys
    dim_platform = dlt.read(dim_platform_table)
    dim_trend_type = dlt.read(dim_trend_type_table)
    dim_feedback = dlt.read(dim_client_feedback_table)
    dim_msm = dlt.read(dim_mainstream_media_table)
    
    # Keep original risk and reach estimation values
    trends_with_estimations = trends_df.select(
        col("*"),
        col("risk_estimation"),
        col("reach_estimation")
    )
    
    # Join with source platform dimension
    trends_with_platform = trends_with_estimations.join(
        broadcast(dim_platform),
        trends_with_estimations.source_platform == dim_platform.platform_name,
        "left"
    ).select(
        trends_with_estimations["*"],
        dim_platform.platform_key.alias("source_platform_key")
    )
    
    # Join with trend type dimension
    trends_with_type = trends_with_platform.join(
        broadcast(dim_trend_type),
        (upper(trim(trends_with_platform.trend_type)) == dim_trend_type.trend_type) &
        (upper(trim(trends_with_platform.risk_type)) == dim_trend_type.risk_type) &
        (upper(trim(trends_with_platform.trend_presentation_method)) == dim_trend_type.trend_presentation_method),
        "left"
    ).select(
        trends_with_platform["*"],
        dim_trend_type.trend_type_key.alias("trend_type_key")
    )
    
    # Join with client feedback dimension
    trends_with_feedback = trends_with_type.join(
        broadcast(dim_feedback),
        upper(trim(trends_with_type.client_feedback)) == dim_feedback.client_feedback,
        "left"
    ).select(
        trends_with_type["*"],
        dim_feedback.feedback_key.alias("feedback_key")
    )
    
    # Join with mainstream media dimension
    trends_with_msm = trends_with_feedback.join(
        broadcast(dim_msm),
        regexp_extract(trends_with_feedback.msm_link, r"https?://(?:www\.)?([^/]+)", 1) == dim_msm.msm_domain,
        "left"
    ).select(
        trends_with_feedback["*"],
        dim_msm.msm_key.alias("msm_key")
    )
    
    # Create final fact table
    fact_df = trends_with_msm.select(
        # Natural Keys
        col("trend_id"),
        col("external_trend_id"),
        
        # Date dimension keys
        col("internal_date_reported").alias("internal_date_reported_key"),
        col("external_date_reported").alias("external_date_reported_key"),
        col("date_published_link").alias("date_published_key"),
        col("msm_date").alias("msm_date_key"),
        col("msm_key"),
        col("client_feedback_date").alias("feedback_date_key"),
        
        # Other dimension keys
        col("source_platform_key"),
        col("trend_type_key"),
        col("feedback_key"),
        
        # Calculate metrics
        when(col("msm_date").isNotNull() & col("internal_date_reported").isNotNull(),
            datediff(col("msm_date"), col("internal_date_reported"))
        ).alias("days_to_msm"),
        
        coalesce(size(col("platform_spread")), lit(0)).alias("platform_spread_count"),
        coalesce(size(col("potential_products_impacted")), lit(0)).alias("google_products_impacted_count"),
        coalesce(size(col("related_trend_ids")), lit(0)).alias("related_trends_count"),
        coalesce(size(col("keywords")), lit(0)).alias("keywords_count"),
        coalesce(size(col("content_tags")), lit(0)).alias("content_tags_count"),
        
        # Risk and reach estimations (original values)
        col("risk_estimation"),
        col("reach_estimation"),
        
        # Flags
        col("msm_date").isNotNull().alias("has_msm_coverage"),
        col("client_feedback").isNotNull().alias("has_client_feedback"),
        (size(col("potential_products_impacted")) > 0).alias("has_google_product_impact"),
        (size(col("platform_spread")) > 1).alias("has_multiple_platforms"),
        (size(col("screenshot_media_links")) > 0).alias("has_screenshots"),
        
        # Descriptive attributes
        col("trend_name"),
        col("trend_description"),
        col("primary_language"),
        col("additional_information"),
        col("example_links"),
        col("screenshot_media_links"),
        col("screenshot_folder_link"),
        col("msm_link"),
        col("analyst"),
        col("country"),
        col("region"),
        
        # Timestamps
        col("created_at"),
        col("last_updated")
    )
    
    return fact_df
