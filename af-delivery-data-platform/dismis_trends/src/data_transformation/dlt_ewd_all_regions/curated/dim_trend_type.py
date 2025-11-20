from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import dlt
from pyspark.sql.functions import col, row_number, trim, upper, md5, concat, coalesce, lit
from pyspark.sql.window import Window
import sys
sys.path.append('..')
from config import dim_trend_type_table, trends_latest_table_name

# Schema for trend type dimension
trend_type_dim_schema = StructType([
    StructField("trend_type_key", IntegerType(), False, metadata={"comment": "Trend type surrogate key"}),
    StructField("trend_type", StringType(), True, metadata={"comment": "Trend type/genre"}),
    StructField("risk_type", StringType(), True, metadata={"comment": "Risk/abuse type"}),
    StructField("trend_presentation_method", StringType(), True, metadata={"comment": "Presentation method (video, image, narrative)"}),
    StructField("trend_type_hash", StringType(), True, metadata={"comment": "Hash for trend type natural key"})
])

@dlt.table(
    name=dim_trend_type_table,
    comment="Trend type dimension table for categorization analysis",
    schema=trend_type_dim_schema,
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def dim_trend_type():
    """
    Create trend type dimension table from trend categorization data.
    Supports trend type analysis and risk categorization.
    """
    # Get distinct combinations of trend type attributes
    trend_types = (
        dlt.read(trends_latest_table_name)
        .select(
            upper(trim(col("trend_type"))).alias("trend_type"),
            upper(trim(col("risk_type"))).alias("risk_type"),
            upper(trim(col("trend_presentation_method"))).alias("trend_presentation_method")
        )
        .filter(
            (col("trend_type").isNotNull()) | 
            (col("risk_type").isNotNull()) | 
            (col("trend_presentation_method").isNotNull())
        )
        .distinct()
    )
    
    # Add hash for natural key
    trend_types_with_hash = trend_types.select(
        col("trend_type"),
        col("risk_type"),
        col("trend_presentation_method"),
        md5(concat(
            coalesce(col("trend_type"), lit("")),
            coalesce(col("risk_type"), lit("")),
            coalesce(col("trend_presentation_method"), lit(""))
        )).alias("trend_type_hash")
    )
    
    # Add surrogate key
    window_spec = Window.orderBy("trend_type", "risk_type", "trend_presentation_method")
    final_df = trend_types_with_hash.select(
        row_number().over(window_spec).alias("trend_type_key"),
        col("trend_type"),
        col("risk_type"),
        col("trend_presentation_method"),
        col("trend_type_hash")
    )
    
    return final_df
