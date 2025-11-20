from databricks.sdk.runtime import spark
    

# Get the bucket name from pipeline parameters, with a default fallback
catalog = spark.conf.get("catalog")
environment = spark.conf.get("environment")
vertical = spark.conf.get("vertical")
data_domain = spark.conf.get("data_domain")
data_source = spark.conf.get("data_source")
file_name_prefix = spark.conf.get("file_name_prefix")


SOURCE_PATH = (
    f"/Volumes/af_delivery_{environment}/data_collection/"
    f"{vertical}/{data_domain}/{data_source}/"
    "*/*/*/" # Year/Month/Day
)

FILE_NAME_PATTERN = f"{file_name_prefix}_[0-9]*.json"

landing_cdc_table_name = f"landing_{vertical}.{data_domain}_{file_name_prefix}"
cleansed_cdc_table_name = f"cleansed_{vertical}.{data_domain}_{file_name_prefix}"
cleansed_latest_table_name = f"cleansed_{vertical}.{data_domain}_{file_name_prefix}_latest"

# Curated layer table names
curated_schema = f"curated_{vertical}"
curated_dim_geo_table_name = f"{curated_schema}.dim_geo"
curated_dim_trends_table_name = f"{curated_schema}.dim_trends"
curated_dim_language_table_name = f"{curated_schema}.dim_language"
