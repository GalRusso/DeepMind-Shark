from databricks.sdk.runtime import spark

# Get the bucket name from pipeline parameters, with a default fallback
catalog = spark.conf.get("catalog")
environment = spark.conf.get("environment")
vertical = spark.conf.get("vertical")

# Consolidated table names
consolidated_latest_table_name = f"consolidated_{vertical}.genai_t2t_latest"

# Source table mappings for all genai modules
genai_latest_tables = {
    "bard": f"cleansed_{vertical}.bard_latest",
    "cohere": f"cleansed_{vertical}.cohere_latest",
    "deepmind": f"cleansed_{vertical}.deepmind_latest",
    "agi": f"cleansed_{vertical}.agi_latest",
}
