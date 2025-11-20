from databricks.sdk.runtime import spark

# Get pipeline parameters
catalog = spark.conf.get("catalog")
environment = spark.conf.get("environment")
vertical = spark.conf.get("vertical")

# Source table names (existing regional latest tables)
regional_trends_latest_tables = {
    "us": f"{catalog}.cleansed_{vertical}.ewd_trends_us_latest",
    "emea": f"{catalog}.cleansed_{vertical}.ewd_trends_emea_latest", 
    "apac": f"{catalog}.cleansed_{vertical}.ewd_trends_apac_latest"
}

regional_copies_latest_tables = {
    "us": f"{catalog}.cleansed_{vertical}.ewd_copies_us_latest",
    "emea": f"{catalog}.cleansed_{vertical}.ewd_copies_emea_latest",
    "apac": f"{catalog}.cleansed_{vertical}.ewd_copies_apac_latest"
}

# Target table names (consolidated latest tables)
trends_latest_table_name = f"{catalog}.consolidated_{vertical}.ewd_trends_latest"
copies_latest_table_name = f"{catalog}.consolidated_{vertical}.ewd_copies_latest"

# Curated layer - Dimension tables
dim_date_table = f"{catalog}.curated_{vertical}.dim_date"
dim_platform_table = f"{catalog}.curated_{vertical}.dim_platform"
dim_trend_type_table = f"{catalog}.curated_{vertical}.dim_trend_type"
dim_client_feedback_table = f"{catalog}.curated_{vertical}.dim_client_feedback"
dim_artifact_status_table = f"{catalog}.curated_{vertical}.dim_artifact_status"
dim_mainstream_media_table = f"{catalog}.curated_{vertical}.dim_mainstream_media"

# Curated layer - Fact tables
fact_trends_table = f"{catalog}.curated_{vertical}.fact_trends"
fact_copies_table = f"{catalog}.curated_{vertical}.fact_copies"

# Curated layer - Bridge tables
bridge_trend_platform_spread_table = f"{catalog}.curated_{vertical}.bridge_trend_platform_spread"
bridge_trend_google_products_table = f"{catalog}.curated_{vertical}.bridge_trend_google_products"
bridge_trend_keywords_table = f"{catalog}.curated_{vertical}.bridge_trend_keywords"
bridge_trend_content_tags_table = f"{catalog}.curated_{vertical}.bridge_trend_content_tags"
bridge_trend_internal_tags_table = f"{catalog}.curated_{vertical}.bridge_trend_internal_tags"
