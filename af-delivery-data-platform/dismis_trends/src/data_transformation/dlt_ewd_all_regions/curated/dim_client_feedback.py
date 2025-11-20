from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import dlt
from pyspark.sql.functions import col, row_number, when, upper, trim, md5
from pyspark.sql.window import Window
import sys
sys.path.append('..')
from config import dim_client_feedback_table, trends_latest_table_name

# Schema for client feedback dimension
client_feedback_dim_schema = StructType([
    StructField("feedback_key", IntegerType(), False, metadata={"comment": "Client feedback surrogate key"}),
    StructField("client_feedback", StringType(), True, metadata={"comment": "Raw client feedback"}),
    StructField("feedback_category", StringType(), True, metadata={"comment": "Feedback category"}),
    StructField("is_relevant_actionable", BooleanType(), True, metadata={"comment": "Flag for relevant & actionable feedback"}),
    StructField("is_positive_feedback", BooleanType(), True, metadata={"comment": "Flag for positive feedback"}),
    StructField("feedback_hash", StringType(), True, metadata={"comment": "Hash for feedback natural key"})
])

@dlt.table(
    name=dim_client_feedback_table,
    comment="Client feedback dimension table for feedback analysis",
    schema=client_feedback_dim_schema,
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def dim_client_feedback():
    """
    Create client feedback dimension table from feedback data.
    Supports client feedback analysis and actionability metrics.
    """
    # Get distinct client feedback values
    feedback_values = (
        dlt.read(trends_latest_table_name)
        .select(upper(trim(col("client_feedback"))).alias("client_feedback"))
        .filter(col("client_feedback").isNotNull())
        .distinct()
    )
    
    # Add feedback attributes
    feedback_with_attributes = feedback_values.select(
        col("client_feedback"),
        
        # Categorize feedback
        when(col("client_feedback").rlike("(?i)(relevant.*actionable)"), "Relevant & Actionable")
        .when(col("client_feedback").rlike("(?i)(relevant.*already.*known)"), "Relevant But Already Known")
        .when(col("client_feedback").rlike("(?i)(flag.*accepted.*relevant)"), "Flag Accepted, Relevant")
        .when(col("client_feedback").rlike("(?i)(flag.*rejected.*not.*relevant)"), "Flag Rejected, Not Relevant")
        .when(col("client_feedback").rlike("(?i)(not.*relevant)"), "Not Relevant")
        .when(col("client_feedback").rlike("(?i)(format.*error)"), "Format Error")
        .otherwise("Other").alias("feedback_category"),
        
        # Flag for relevant & actionable
        when(col("client_feedback").rlike("(?i)(relevant.*actionable|actionable.*relevant)"), True)
        .otherwise(False).alias("is_relevant_actionable"),
        
        # Flag for positive feedback
        when(col("client_feedback").rlike("(?i)(relevant.*actionable|good|excellent|helpful|useful|valuable)"), True)
        .when(col("client_feedback").rlike("(?i)(not.*relevant|not.*actionable|poor|bad|unhelpful|useless)"), False)
        .otherwise(False).alias("is_positive_feedback"),
        
        md5(col("client_feedback")).alias("feedback_hash")
    )
    
    # Add surrogate key
    window_spec = Window.orderBy("client_feedback")
    final_df = feedback_with_attributes.select(
        row_number().over(window_spec).alias("feedback_key"),
        col("client_feedback"),
        col("feedback_category"),
        col("is_relevant_actionable"),
        col("is_positive_feedback"),
        col("feedback_hash")
    )
    
    return final_df
