"""Update Monday board items status after successful Pinecone sync"""

import argparse
import asyncio
import sys

import httpx
import nest_asyncio
from aiolimiter import AsyncLimiter
from constants import MONDAY_TRACKING_SCHEMA, get_table_names
from databricks.sdk.runtime import dbutils, spark
from pyspark.sql.functions import col
from tqdm import tqdm

# Add shared library path
sys.path.append("../../../shared")
from libs import monday

nest_asyncio.apply()

# Monday configuration
STATUS_COLUMN_ID = "color_mkwzft7m"
STATUS_LABEL = "Processed"
RATE_LIMIT = AsyncLimiter(max_rate=140, time_period=60)


def get_last_monday_update_timestamp(environment: str):
    """Get the last Monday update timestamp from tracking table"""
    tracking_table_name = get_table_names(environment)["monday_tracking"]
    tracking_df = spark.table(tracking_table_name)
    tracking_record = tracking_df.first()

    if tracking_record is None:
        return None

    return tracking_record.last_monday_update_timestamp


def update_monday_tracking(environment: str, items_count: int):
    """Update Monday tracking table after successful update"""
    from datetime import UTC, datetime

    tracking_table_name = get_table_names(environment)["monday_tracking"]
    tracking_data = [(datetime.now(UTC), items_count)]

    new_tracking_df = spark.createDataFrame(tracking_data, MONDAY_TRACKING_SCHEMA)
    new_tracking_df.write.mode("overwrite").format("delta").saveAsTable(tracking_table_name)


def get_monday_items_from_gold(environment: str, incremental: bool = True) -> list[tuple[str, str, str]]:
    """Query gold table for Monday items that need status update"""
    gold_table_name = get_table_names(environment)["gold"]

    items_df = spark.table(gold_table_name)

    if incremental:
        last_updated = get_last_monday_update_timestamp(environment)
        if last_updated:
            print(f"Incremental mode: Processing items with chunks created after {last_updated}")
            items_df = items_df.where(col("created_at") > last_updated)
        else:
            print("No tracking record - processing all items")
    else:
        print("Full sync mode - processing all items")

    items_df = items_df.select("board_id", "group_id", "item_id").distinct()

    items = [(row.board_id, row.group_id, row.item_id) for row in items_df.collect()]
    print(f"Found {len(items)} Monday items to update")
    return items


async def update_single_item(
    api_key: str,
    board_id: str,
    group_id: str,
    item_id: str,
    session: httpx.AsyncClient,
) -> tuple[bool, str, str, str, str | None]:
    """
    Update a single Monday item with "Processed" status

    Returns:
        Tuple of (success, board_id, group_id, item_id, error_message)
    """
    async with RATE_LIMIT:
        try:
            column_values = {STATUS_COLUMN_ID: {"label": STATUS_LABEL}}

            result = await monday.aupdate_item(
                api_key=api_key,
                item_id=item_id,
                column_values=column_values,
                board_id=board_id,
                session=session,
            )
            return (True, board_id, group_id, item_id, None)
        except Exception as e:
            error_msg = str(e)
            return (False, board_id, group_id, item_id, error_msg)


async def update_monday_items_async(api_key: str, items: list[tuple[str, str, str]]):
    """
    Update all Monday items with async concurrency control

    Args:
        api_key: Monday API key
        items: List of (board_id, group_id, item_id) tuples
    """
    failed_updates = []

    async with httpx.AsyncClient(timeout=3000.0) as session:
        tasks = [
            update_single_item(api_key, board_id, group_id, item_id, session) for board_id, group_id, item_id in items
        ]

        # Use tqdm for progress tracking
        pbar = tqdm(total=len(tasks), desc="Updating Monday items")

        for coro in asyncio.as_completed(tasks):
            success, board_id, group_id, item_id, error = await coro
            pbar.update(1)

            if not success:
                failed_updates.append((board_id, group_id, item_id, error))

        pbar.close()

    # Print summary
    print("\nMonday update complete:")
    print(f"  Success: {len(items) - len(failed_updates)}")
    print(f"  Failed: {len(failed_updates)}")

    if failed_updates:
        print("\nFailed updates:")
        for board_id, group_id, item_id, error in failed_updates[:10]:  # Show first 10
            print(f"  - board_id={board_id}, group_id={group_id}, item_id={item_id}")
            print(f"    Error: {error}")
        if len(failed_updates) > 10:
            print(f"  ... and {len(failed_updates) - 10} more failures")
        raise Exception("Failed to update Monday items")


def main(environment: str, skip_monday_update: bool = False, incremental: bool = True):
    """Main entry point for Monday status updates"""
    if skip_monday_update:
        print("Skipping Monday status update (--skip_monday_update flag is set)")
        return

    print(f"Starting Monday board status updates ({'incremental' if incremental else 'full sync'} mode)")
    api_key = dbutils.secrets.get("datn", "monday_api_key")

    items = get_monday_items_from_gold(environment, incremental)

    if not items:
        print("No Monday items found to update")
        return

    asyncio.run(update_monday_items_async(api_key, items))

    # Update tracking after successful completion
    update_monday_tracking(environment, len(items))

    print("Monday board status updates completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="update_monday_status")
    parser.add_argument(
        "--environment",
        required=True,
        choices=["dev", "prod"],
        help="Environment (dev/prod)",
    )
    parser.add_argument(
        "--skip_monday_update",
        required=False,
        help="Skip Monday status update (any non-empty value will skip)",
    )
    parser.add_argument(
        "--full_sync",
        required=False,
        help="Force full sync instead of incremental (any non-empty value)",
    )
    args = parser.parse_args()

    skip = bool(args.skip_monday_update)
    full_sync = bool(args.full_sync)
    incremental = not full_sync
    main(environment=args.environment, skip_monday_update=skip, incremental=incremental)
