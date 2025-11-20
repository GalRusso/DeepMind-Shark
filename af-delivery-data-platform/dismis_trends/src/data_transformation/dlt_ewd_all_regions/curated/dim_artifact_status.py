from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import dlt
from pyspark.sql.functions import col, row_number, when, upper, trim, md5
from pyspark.sql.window import Window
import sys
sys.path.append('..')
from config import dim_artifact_status_table, copies_latest_table_name

# Schema for artifact status dimension
artifact_status_dim_schema = StructType([
    StructField("artifact_status_key", IntegerType(), False, metadata={"comment": "Artifact status surrogate key"}),
    StructField("artifact_status", StringType(), True, metadata={"comment": "Artifact status"}),
    StructField("status_category", StringType(), True, metadata={"comment": "Status category"}),
    StructField("is_actionable", BooleanType(), True, metadata={"comment": "Flag for actionable status (restricted/offline)"}),
    StructField("is_active", BooleanType(), True, metadata={"comment": "Flag for active/online status"}),
    StructField("status_hash", StringType(), True, metadata={"comment": "Hash for status natural key"})
])

@dlt.table(
    name=dim_artifact_status_table,
    comment="Artifact status dimension table for artifact actionability analysis",
    schema=artifact_status_dim_schema,
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def dim_artifact_status():
    """
    Create artifact status dimension table from copies data.
    Supports artifact actionability analysis.
    """
    # Get distinct artifact status values
    status_values = (
        dlt.read(copies_latest_table_name)
        .select(upper(trim(col("artifact_status"))).alias("artifact_status"))
        .filter(col("artifact_status").isNotNull())
        .distinct()
    )
    
    # Add status attributes
    status_with_attributes = status_values.select(
        col("artifact_status"),
        
        # Categorize status
        when(col("artifact_status").rlike("(?i)(online)"), "Online")
        .when(col("artifact_status").rlike("(?i)(offline)"), "Offline")
        .when(col("artifact_status").rlike("(?i)(restricted)"), "Restricted")
        .otherwise("Other").alias("status_category"),
        
        # Flag for actionable (restricted or offline)
        when(col("artifact_status").rlike("(?i)(restricted|offline)"), True)
        .otherwise(False).alias("is_actionable"),
        
        # Flag for active/online
        when(col("artifact_status").rlike("(?i)(online)"), True)
        .when(col("artifact_status").rlike("(?i)(offline|restricted)"), False)
        .otherwise(False).alias("is_active"),
        
        md5(col("artifact_status")).alias("status_hash")
    )
    
    # Add surrogate key
    window_spec = Window.orderBy("artifact_status")
    final_df = status_with_attributes.select(
        row_number().over(window_spec).alias("artifact_status_key"),
        col("artifact_status"),
        col("status_category"),
        col("is_actionable"),
        col("is_active"),
        col("status_hash")
    )
    
    return final_df
