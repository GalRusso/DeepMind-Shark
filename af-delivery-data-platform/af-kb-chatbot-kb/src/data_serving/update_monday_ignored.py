"""
Script to update Monday.com board items for ignored files.

This script identifies files that were intentionally ignored during processing
(e.g., Excel files > 1MB) and updates the corresponding Monday.com items with
a message explaining why they were ignored.

The script:
1. Queries the ignored_files table from landing
2. Joins with file_metadata to get item_id
3. Updates Monday items with the ignore reason using the same error column as DLQ
"""

import argparse
import asyncio
import sys

import httpx
import nest_asyncio
import pandas as pd
from aiolimiter import AsyncLimiter
from constants import get_table_names
from databricks.sdk.runtime import dbutils, spark
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

sys.path.append("../../../shared")

nest_asyncio.apply()

# Monday board configuration
DEFAULT_MONDAY_BOARD_ID = "18221691675"
ERROR_COLUMN_ID = "text_mkxd8p3c"  # Same error column as used by update_monday_error.py
RATE_LIMIT = AsyncLimiter(max_rate=140, time_period=60)

IGNORE_TABLE_NAME = "ignored_files"


def get_ignored_items_with_metadata(environment: str) -> pd.DataFrame:
    """
    Get ignored files and join with metadata to get item_id.

    Args:
        environment: The environment (dev or prod)

    Returns:
        Pandas DataFrame with columns: item_id, file_name, file_size, ignore_reason, ingestion_ts
    """
    table_names = get_table_names(environment)

    # Get ignored files from landing
    ignored_df = spark.table(f"{table_names['landing_schema']}.ignored_files")

    # Get file metadata to map s3_root_name to item_id
    metadata_df = spark.table(f"{table_names['landing_schema']}.file_metadata")

    # Join to get item_id
    result_df = (
        ignored_df.join(metadata_df.select("s3_root_name", "item_id"), on="s3_root_name", how="inner")
        # Get only the latest ignored file per item_id (in case of multiple ingestions)
        .withColumn("row_num", row_number().over(Window.partitionBy("item_id").orderBy(col("ingestion_ts").desc())))
        .filter(col("row_num") == 1)
        .select(col("item_id").cast("string"), "file_name", "file_size", "ignore_reason", "ingestion_ts")
    )

    return result_df.toPandas()


async def update_single_item(
    session: httpx.AsyncClient,
    api_key: str,
    board_id: str,
    item_id: str,
    message: str,
) -> tuple[str, bool, str]:
    """
    Update a single Monday.com item with an ignore message.

    Args:
        session: HTTP client session
        api_key: Monday API key
        board_id: Monday board ID
        item_id: Item ID to update
        message: Message to set in the error column

    Returns:
        Tuple of (item_id, success, error_message)
    """
    async with RATE_LIMIT:
        try:
            query = """
            mutation ($boardId: ID!, $itemId: ID!, $columnId: String!, $value: String!) {
                change_simple_column_value(
                    board_id: $boardId,
                    item_id: $itemId,
                    column_id: $columnId,
                    value: $value
                ) {
                    id
                }
            }
            """

            variables = {
                "boardId": board_id,
                "itemId": item_id,
                "columnId": ERROR_COLUMN_ID,
                "value": message,
            }

            response = await session.post(
                "https://api.monday.com/v2",
                json={"query": query, "variables": variables},
                headers={
                    "Authorization": api_key,
                    "Content-Type": "application/json",
                },
                timeout=30.0,
            )

            response.raise_for_status()
            result = response.json()

            if "errors" in result:
                error_msg = str(result["errors"])
                return item_id, False, error_msg

            return item_id, True, ""

        except Exception as e:
            return item_id, False, str(e)


async def update_monday_items_async(
    api_key: str,
    board_id: str,
    items_df: pd.DataFrame,
) -> tuple[int, int]:
    """
    Update Monday items asynchronously with ignore messages.

    Args:
        api_key: Monday API key
        board_id: Monday board ID
        items_df: DataFrame with columns: item_id, ignore_reason

    Returns:
        Tuple of (success_count, error_count)
    """
    if items_df.empty:
        print("No items to update")
        return 0, 0

    print(f"Updating {len(items_df)} items...")

    async with httpx.AsyncClient() as session:
        tasks = [
            update_single_item(session, api_key, board_id, str(row["item_id"]), row["ignore_reason"])
            for _, row in items_df.iterrows()
        ]

        results = await asyncio.gather(*tasks)

    # Count successes and failures
    success_count = sum(1 for _, success, _ in results if success)
    error_count = len(results) - success_count

    # Print failed updates
    failed_updates = [(item_id, error) for item_id, success, error in results if not success]
    if failed_updates:
        print(f"\n⚠️  {error_count} items failed to update:")
        for item_id, error in failed_updates[:5]:  # Show first 5
            print(f"    - Item {item_id}: {error}")
        if len(failed_updates) > 5:
            print(f"    ... and {len(failed_updates) - 5} more failures")

    return success_count, error_count


def update_monday_items_with_ignored_files(environment: str):
    """
    Main function to update Monday.com items with ignore messages.

    Args:
        environment: The environment (dev or prod)
    """
    print(f"Starting Monday ignored files update for environment: {environment}")
    print("=" * 80)

    # Get Monday API key from secrets
    monday_api_key = dbutils.secrets.get("datn", "monday_api_key")
    print("Retrieved Monday API key from secrets")
    print()

    # Get ignored items with metadata
    print("Querying ignored files with metadata...")
    print("-" * 80)
    ignored_df = get_ignored_items_with_metadata(environment)

    if ignored_df.empty:
        print("✓ No ignored files found")
        print()
        return

    print(f"Found {len(ignored_df)} ignored files")
    print("Sample ignored files:")
    for _, row in ignored_df.head(5).iterrows():
        print(f"  - Item {row['item_id']}: {row['file_name']} ({row['file_size']:,} bytes) - {row['ignore_reason']}")
    if len(ignored_df) > 5:
        print(f"  ... and {len(ignored_df) - 5} more")
    print()

    # Update Monday items
    print("Updating Monday items with ignore messages...")
    print("-" * 80)
    success_count, error_count = asyncio.run(
        update_monday_items_async(monday_api_key, DEFAULT_MONDAY_BOARD_ID, ignored_df)
    )

    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"✓ Successfully updated: {success_count} items")
    if error_count > 0:
        print(f"✗ Failed to update: {error_count} items")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update Monday.com items with ignore messages for filtered files")
    parser.add_argument(
        "--env",
        type=str,
        choices=["dev", "prod"],
        default="dev",
        help="Environment to run in (dev or prod)",
    )

    args = parser.parse_args()

    update_monday_items_with_ignored_files(args.env)
