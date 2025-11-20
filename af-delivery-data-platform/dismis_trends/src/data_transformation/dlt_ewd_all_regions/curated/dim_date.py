from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType, BooleanType
import dlt
from pyspark.sql.functions import (
    col, year, month, quarter, dayofweek, dayofmonth, dayofyear,
    weekofyear, date_format, when, last_day
)
from datetime import datetime, timedelta
import sys
sys.path.append('..')
from config import dim_date_table
from databricks.sdk.runtime import spark

# Schema for date dimension
date_dim_schema = StructType([
    StructField("date_key", DateType(), False, metadata={"comment": "Date key (primary key)"}),
    StructField("year", IntegerType(), True, metadata={"comment": "Year (YYYY)"}),
    StructField("quarter", IntegerType(), True, metadata={"comment": "Quarter (1-4)"}),
    StructField("month", IntegerType(), True, metadata={"comment": "Month (1-12)"}),
    StructField("month_name", StringType(), True, metadata={"comment": "Month name (January-December)"}),
    StructField("month_short", StringType(), True, metadata={"comment": "Month abbreviation (Jan-Dec)"}),
    StructField("week_of_year", IntegerType(), True, metadata={"comment": "Week of year (1-53)"}),
    StructField("day_of_month", IntegerType(), True, metadata={"comment": "Day of month (1-31)"}),
    StructField("day_of_week", IntegerType(), True, metadata={"comment": "Day of week (1-7, Sunday=1)"}),
    StructField("day_name", StringType(), True, metadata={"comment": "Day name (Sunday-Saturday)"}),
    StructField("day_short", StringType(), True, metadata={"comment": "Day abbreviation (Sun-Sat)"}),
    StructField("day_of_year", IntegerType(), True, metadata={"comment": "Day of year (1-366)"}),
    StructField("is_weekend", BooleanType(), False, metadata={"comment": "Weekend flag (Saturday/Sunday)"}),
    StructField("is_month_start", BooleanType(), False, metadata={"comment": "First day of month flag"}),
    StructField("is_month_end", BooleanType(), False, metadata={"comment": "Last day of month flag"}),
    StructField("fiscal_year", IntegerType(), True, metadata={"comment": "Fiscal year"}),
    StructField("fiscal_quarter", IntegerType(), True, metadata={"comment": "Fiscal quarter"}),
    StructField("year_month", StringType(), True, metadata={"comment": "Year-Month (YYYY-MM)"}),
    StructField("year_quarter", StringType(), True, metadata={"comment": "Year-Quarter (YYYY-Q#)"})
])

@dlt.table(
    name=dim_date_table,
    comment="Date dimension table for time-based analysis",
    schema=date_dim_schema,
    table_properties={
        "quality": "gold"
    }
)
def dim_date():
    """
    Create date dimension table covering dates from 2020 to 2030.
    Supports temporal analysis for all trend-related use cases.
    """
    # Generate date range
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2030, 12, 31)
    
    # Create date sequence
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append((current_date,))
        current_date += timedelta(days=1)
    
    # Create DataFrame
    df = spark.createDataFrame(dates, ["date_key"])
    
    # Add date attributes
    df = df.select(
        col("date_key").cast(DateType()).alias("date_key"),  # Ensure date_key is DateType
        year("date_key").alias("year"),
        quarter("date_key").alias("quarter"),
        month("date_key").alias("month"),
        date_format("date_key", "MMMM").alias("month_name"),
        date_format("date_key", "MMM").alias("month_short"),
        weekofyear("date_key").alias("week_of_year"),
        dayofmonth("date_key").alias("day_of_month"),
        dayofweek("date_key").alias("day_of_week"),
        date_format("date_key", "EEEE").alias("day_name"),
        date_format("date_key", "EEE").alias("day_short"),
        dayofyear("date_key").alias("day_of_year"),
        when(dayofweek("date_key").isin([1, 7]), True).otherwise(False).alias("is_weekend"),
        when(dayofmonth("date_key") == 1, True).otherwise(False).alias("is_month_start"),
        when(col("date_key") == last_day("date_key"), True).otherwise(False).alias("is_month_end"),
        # Fiscal year (assuming fiscal year starts in January)
        year("date_key").alias("fiscal_year"),
        quarter("date_key").alias("fiscal_quarter"),
        date_format("date_key", "yyyy-MM").alias("year_month"),
        date_format("date_key", "yyyy-'Q'Q").alias("year_quarter")
    )
    
    return df
