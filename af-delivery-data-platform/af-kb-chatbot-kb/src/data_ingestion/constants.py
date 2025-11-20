from databricks.sdk.runtime import spark
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

VERTICAL = "kb_unstructured"


def get_table_names(environment: str) -> dict:
    """
    Get table names for the given environment.

    Args:
        environment: The environment (dev or prod)

    Returns:
        Dictionary with catalog, namespace, and table names
    """
    catalog = f"af_delivery_{environment}"
    namespace = f"{catalog}.ingestion_{VERTICAL}"
    return {
        "catalog": catalog,
        "namespace": namespace,
        "monday_discovery_table": f"{namespace}.monday_discovery",
        "monday_tracking_table": f"{namespace}.monday_ggdrive_download_tracking",
        "monday_download_dlq": f"{namespace}.monday_download_dlq",
    }


MONDAY_DISCOVERY_SCHEMA = StructType(
    [
        StructField("board_id", StringType(), True, metadata={"comment": "Monday board_id"}),
        StructField("group_id", StringType(), True, metadata={"comment": "Monday group_id"}),
        StructField("item_id", StringType(), True, metadata={"comment": "Monday item_id"}),
        StructField("file_name", StringType(), True, metadata={"comment": "Name of the file"}),
        StructField("gdrive_path", StringType(), True, metadata={"comment": "Google Drive path"}),
        StructField("created_at", TimestampType(), True, metadata={"comment": "When we first saw this revision"}),
        StructField("metadata", StringType(), True, metadata={"comment": "Metadata when discovering this records"}),
    ]
)


MONDAY_TRACKING_SCHEMA = StructType(
    [
        StructField("board_id", StringType(), True, metadata={"comment": "Monday board_id"}),
        StructField("group_id", StringType(), True, metadata={"comment": "Monday group_id"}),
        StructField("item_id", StringType(), True, metadata={"comment": "Monday item_id"}),
        StructField("gdrive_path", StringType(), True, metadata={"comment": "Google Drive path"}),
        StructField(
            "original_path",
            StringType(),
            True,
            metadata={"comment": "Path after landing, e.g. 's3://...<file_name>.docx'"},
        ),
        StructField(
            "metadata_path",
            StringType(),
            True,
            metadata={"comment": "JSON file with metadata, e.g. 's3://...<file_name>.docx.metadata.json'"},
        ),
        StructField(
            "created_at",
            TimestampType(),
            True,
            metadata={"comment": "When artifacts were successfully written to s3"},
        ),
        StructField(
            "file_revision_id",
            StringType(),
            True,
            metadata={"comment": "Google Drive headRevisionId"},
        ),
    ]
)


MONDAY_DLQ_SCHEMA = StructType(
    [
        StructField("board_id", StringType(), True, metadata={"comment": "Monday board_id"}),
        StructField("group_id", StringType(), True, metadata={"comment": "Monday group_id"}),
        StructField("item_id", StringType(), True, metadata={"comment": "Monday item_id"}),
        StructField("file_name", StringType(), True, metadata={"comment": "Human-readable file name"}),
        StructField(
            "delivery_date", StringType(), True, metadata={"comment": "Delivery date from Monday.com metadata"}
        ),
        StructField("gdrive_path", StringType(), True, metadata={"comment": "Google Drive path"}),
        StructField("error_message", StringType(), True, metadata={"comment": "Error message details"}),
        StructField("error_type", StringType(), True, metadata={"comment": "Exception class name"}),
        StructField("error_timestamp", TimestampType(), True, metadata={"comment": "When the error occurred"}),
    ]
)


def ensure_tables_exist(environment: str):
    """
    Ensure that the required tables exist in the database.

    Args:
        environment: The environment (dev or prod)
    """
    table_names = get_table_names(environment)

    spark.createDataFrame([], MONDAY_DISCOVERY_SCHEMA).write.mode("ignore").format("delta").saveAsTable(
        table_names["monday_discovery_table"]
    )
    spark.createDataFrame([], MONDAY_TRACKING_SCHEMA).write.mode("ignore").format("delta").saveAsTable(
        table_names["monday_tracking_table"]
    )
    spark.createDataFrame([], MONDAY_DLQ_SCHEMA).write.mode("ignore").format("delta").saveAsTable(
        table_names["monday_download_dlq"]
    )
