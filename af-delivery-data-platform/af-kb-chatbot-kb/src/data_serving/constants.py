from databricks.sdk.runtime import spark
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

VERTICAL = "kb_unstructured"
SOURCE = "monday"


GOLD_TRACKING_SCHEMA = StructType(
    [
        StructField(
            "last_processed_timestamp", TimestampType(), False, metadata={"comment": "Timestamp of last processed data"}
        ),
        StructField("pinecone_stats", StringType(), True, metadata={"comment": "Stats of pinecone, it's fun to watch"}),
    ]
)


MONDAY_TRACKING_SCHEMA = StructType(
    [
        StructField(
            "last_monday_update_timestamp",
            TimestampType(),
            False,
            metadata={"comment": "Timestamp of last Monday status update"},
        ),
        StructField(
            "items_updated_count",
            IntegerType(),
            True,
            metadata={"comment": "Number of Monday items updated in last run"},
        ),
    ]
)


def get_table_names(environment: str) -> dict:
    """
    Get table names for the given environment.

    Args:
        environment: The environment (dev or prod)

    Returns:
        Dictionary with catalog, namespace, and table names
    """
    catalog = f"af_delivery_{environment}"
    namespace = f"{catalog}.gold_{VERTICAL}"
    return {
        "catalog": catalog,
        "namespace": namespace,
        "gold": f"{namespace}.gold",
        "gold_tracking": f"{namespace}.gold_tracking",
        "monday_tracking": f"{namespace}.monday_tracking",
        "landing_schema": f"{catalog}.landing_{VERTICAL}",
    }


def ensure_tables_exist(environment: str):
    """
    Ensure that the required tables exist in the database.

    Args:
        environment: The environment (dev or prod)
    """
    table_names = get_table_names(environment)

    spark.createDataFrame([], GOLD_TRACKING_SCHEMA).write.mode("ignore").format("delta").saveAsTable(
        table_names["gold_tracking"]
    )
    spark.createDataFrame([], MONDAY_TRACKING_SCHEMA).write.mode("ignore").format("delta").saveAsTable(
        table_names["monday_tracking"]
    )
