"""
Three-phase script to update Monday.com board items with error messages.

This script identifies and updates various error conditions:

Phase 1 - Clear fixed items:
1. Queries items from tracking table (successfully processed files)
2. Clears error column for these items (they're fixed now)

Phase 2 - Update broken items:
3. Queries currently broken files (files in DLQ that haven't been successfully downloaded)
   using left anti join with tracking table
4. Gets only the latest error per file using window functions
5. Updates the corresponding Monday.com board items with formatted error messages

Phase 3 - Update duplicate items:
6. Queries unprocessed items that duplicate already-processed items (by final_report_url)
7. Updates these items with a message indicating which item they duplicate

All phases use async API calls with rate limiting for performance.
"""

import argparse
import asyncio
import re
import sys

import httpx
import nest_asyncio
import pandas as pd
from aiolimiter import AsyncLimiter
from constants import get_table_names
from databricks.sdk.runtime import dbutils, spark
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from tqdm import tqdm

sys.path.append("../../../shared")
from libs import monday  # noqa: E402

nest_asyncio.apply()

# Monday board configuration
DEFAULT_MONDAY_BOARD_ID = "18221691675"
ERROR_COLUMN_ID = "text_mkxd8p3c"  # Change this to your error column ID
RATE_LIMIT = AsyncLimiter(max_rate=140, time_period=60)


def get_duplicated_items() -> pd.DataFrame:
    """
    Identify unprocessed items that are duplicates of already-processed items.

    Compares unprocessed items (group_mkwzb81w) with processed items (group_mkwynz1z)
    by matching on final_report_url. For duplicates, adds a 'reason' column with
    a HYPERLINK formula pointing to the processed item.

    Returns:
        DataFrame with all unprocessed items, where duplicates have a non-null 'reason' column.
        The 'id' column contains the unprocessed item ID (used to update Monday).
    """
    api_key = dbutils.secrets.get(scope="datn", key="monday_api_key")

    # Get unprocessed items
    unprocessed_json_data = monday.get_board_items(
        api_key=api_key,
        board_id=DEFAULT_MONDAY_BOARD_ID,
        group_id="group_mkwzb81w",
        exclude_columns=[
            "make_error_message",  # [Make] Error Message, this is used by shani
        ],
        get_subitems=False,
    )
    unprocessed_df = pd.DataFrame(unprocessed_json_data)

    # Get processed items
    processed_json_data = monday.get_board_items(
        api_key=api_key,
        board_id=DEFAULT_MONDAY_BOARD_ID,
        group_id="group_mkwynz1z",
        exclude_columns=[
            "make_error_message",  # [Make] Error Message, this is used by shani
        ],
        get_subitems=False,
    )
    processed_df = pd.DataFrame(processed_json_data)

    # Initialize reason column as None
    unprocessed_df["reason"] = None

    # Create a mapping: final_report_url -> (processed_id, processed_name)
    # Only keep processed items that have a non-null final_report_url
    processed_lookup = (
        processed_df[processed_df["final_report_url"].notna()][["final_report_url", "id", "name"]]
        .drop_duplicates(subset=["final_report_url"], keep="first")
        .set_index("final_report_url")
    )

    # Find duplicates: unprocessed items whose final_report_url exists in processed items
    # Only check unprocessed items with non-null final_report_url
    mask = unprocessed_df["final_report_url"].notna() & unprocessed_df["final_report_url"].isin(processed_lookup.index)

    # For each duplicate, create the reason message
    for idx in unprocessed_df[mask].index:
        report_url = unprocessed_df.loc[idx, "final_report_url"]
        processed_id = processed_lookup.loc[report_url, "id"]
        processed_name = processed_lookup.loc[report_url, "name"]

        monday_url = f"https://activefence-company.monday.com/boards/{DEFAULT_MONDAY_BOARD_ID}/pulses/{processed_id}"

        # Create HYPERLINK formula for Monday.com
        reason_msg = f"duplicated with {processed_name} ({monday_url})"

        unprocessed_df.loc[idx, "reason"] = reason_msg

    # Return unprocessed_df with 'id' column intact (contains unprocessed item IDs)
    # and 'reason' column populated for duplicates
    return unprocessed_df


def get_fixed_items(tracking_table: str) -> DataFrame:
    """
    Query items that have been successfully processed (in tracking table).
    These items should have their error columns cleared.

    Args:
        tracking_table: Tracking table name

    Returns:
        DataFrame with unique (board_id, item_id) from tracking table
    """
    fixed_df = (
        spark.table(tracking_table)
        .select(
            "board_id",
            "item_id",
        )
        .distinct()
    )

    return fixed_df


def get_currently_broken_files(dlq_table: str, tracking_table: str) -> DataFrame:
    """
    Query currently broken files (files in DLQ that haven't been successfully downloaded).
    Returns the latest error per file.

    Args:
        dlq_table: DLQ table name
        tracking_table: Tracking table name

    Returns:
        DataFrame with currently broken files (latest error per file)
    """
    # Get files that are in DLQ but not in tracking (currently broken)
    broken_df = spark.table(dlq_table).join(
        spark.table(tracking_table),
        on=["board_id", "group_id", "item_id"],
        how="left_anti",
    )

    # Use window function to get latest error per file
    window = Window.partitionBy("board_id", "group_id", "item_id").orderBy(col("error_timestamp").desc())
    latest_errors = (
        broken_df.withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
        .select(
            "board_id",
            "group_id",
            "item_id",
            "file_name",
            "delivery_date",
            "gdrive_path",
            "error_message",
            "error_type",
            "error_timestamp",
        )
    )

    return latest_errors


async def clear_single_item_async(
    api_key: str,
    board_id: str,
    item_id: str,
    session: httpx.AsyncClient,
) -> tuple[bool, str, str | None]:
    """
    Clear error column for a single Monday item.

    Returns:
        Tuple of (success, item_id, error_message)
    """
    async with RATE_LIMIT:
        try:
            await monday.aupdate_item(
                api_key=api_key,
                item_id=item_id,
                board_id=board_id,
                column_values={ERROR_COLUMN_ID: ""},
                session=session,
            )
            return (True, item_id, None)
        except Exception as e:
            return (False, item_id, str(e))


async def clear_monday_errors_async(monday_api_key: str, items: list) -> tuple:
    """
    Clear error column for a list of Monday items using async with rate limiting.

    Args:
        monday_api_key: Monday.com API key
        items: List of items (rows with board_id, item_id)

    Returns:
        Tuple of (success_count, error_count)
    """
    failed_clears = []

    async with httpx.AsyncClient(timeout=3000.0) as session:
        tasks = [
            clear_single_item_async(
                monday_api_key,
                row.board_id or DEFAULT_MONDAY_BOARD_ID,
                row.item_id,
                session,
            )
            for row in items
        ]

        # Use tqdm for progress tracking
        pbar = tqdm(total=len(tasks), desc="Clearing fixed items")

        for coro in asyncio.as_completed(tasks):
            success, item_id, error = await coro
            pbar.update(1)

            if not success:
                failed_clears.append((item_id, error))

        pbar.close()

    success_count = len(items) - len(failed_clears)
    error_count = len(failed_clears)

    if failed_clears:
        print(f"\n  X Failed to clear {error_count} items")
        for item_id, error in failed_clears[:5]:  # Show first 5
            print(f"    - Item {item_id}: {error}")
        if len(failed_clears) > 5:
            print(f"    ... and {len(failed_clears) - 5} more failures")

    return success_count, error_count


def format_error_message(error_message: str, error_type: str, max_length: int = 500) -> str:
    """
    Format error messages for display in Monday.com.

    Special handling for:
    1. Duplicate items: Uses the message as-is (already formatted with HYPERLINK)
    2. Mime type errors: Converts unsupported mime type errors to "mime_type='...' is not supported"
       - Old format: "No extension found for name='...', mime_type='...'"
       - New format: "Unsupported file type: mime_type='...'"
    3. Google API errors: Extracts the 'message' field from the Content section
       (handles Forbidden, Internal Server Error, and other API errors)

    Args:
        error_message: The original error message
        error_type: The error type (exception class name)
        max_length: Maximum length for the formatted message

    Returns:
        Formatted error message ready for Monday.com
    """
    # Handle duplicate items - message already contains HYPERLINK formula
    if error_type == "duplicate":
        formatted = error_message
    else:
        # Pattern 1: Match mime type errors (both old and new formats)
        # Old format: "No extension found for name='...', mime_type='...'"
        # New format: "Unsupported file type: mime_type='...'"
        mime_type_pattern = r"(?:No extension found for name='[^']*',\s*mime_type='([^']*)'|Unsupported file type: mime_type='([^']*)')"
        match = re.search(mime_type_pattern, error_message)

        if match:
            # Extract mime_type from whichever group matched
            mime_type = match.group(1) or match.group(2)
            formatted = f"mime_type='{mime_type}' is not supported"
        else:
            # Pattern 2: Match Google API errors with Content section containing 'message'
            # This catches Forbidden, Internal Server Error, and other API errors with same structure
            # Matches: 'message': 'Some message text' or "message": "Some message text"
            api_error_pattern = r"Content:.*['\"]message['\"]:\s*['\"]([^'\"]+)['\"]"
            match = re.search(api_error_pattern, error_message, re.DOTALL)

            if match:
                error_message = match.group(1)
                error_type = "google drive api"
                formatted = f"[{error_type}] {error_message}"
            else:
                # Use original error message with error type prefix
                formatted = f"[{error_type}] {error_message}"

    # Truncate if too long
    if len(formatted) > max_length:
        formatted = formatted[: max_length - 3] + "..."

    return formatted


async def update_single_item_async(
    api_key: str,
    board_id: str,
    item_id: str,
    error_message: str,
    error_type: str,
    session: httpx.AsyncClient,
) -> tuple[bool, str, str | None]:
    """
    Update a single Monday item with formatted error message.

    Returns:
        Tuple of (success, item_id, error_message)
    """
    async with RATE_LIMIT:
        try:
            formatted_error = format_error_message(error_message, error_type)
            await monday.aupdate_item(
                api_key=api_key,
                item_id=item_id,
                board_id=board_id,
                column_values={ERROR_COLUMN_ID: formatted_error},
                session=session,
            )
            return (True, item_id, None)
        except Exception as e:
            return (False, item_id, str(e))


async def update_monday_errors_async(monday_api_key: str, error_items: list) -> tuple:
    """
    Update Monday items with error messages using async with rate limiting.

    Args:
        monday_api_key: Monday.com API key
        error_items: List of error items (rows with board_id, item_id, error_message, error_type)

    Returns:
        Tuple of (success_count, error_count)
    """
    failed_updates = []

    async with httpx.AsyncClient(timeout=3000.0) as session:
        tasks = [
            update_single_item_async(
                monday_api_key,
                row.board_id or DEFAULT_MONDAY_BOARD_ID,
                row.item_id,
                row.error_message,
                row.error_type,
                session,
            )
            for row in error_items
        ]

        # Use tqdm for progress tracking
        pbar = tqdm(total=len(tasks), desc="Updating broken items")

        for coro in asyncio.as_completed(tasks):
            success, item_id, error = await coro
            pbar.update(1)

            if not success:
                failed_updates.append((item_id, error))

        pbar.close()

    success_count = len(error_items) - len(failed_updates)
    error_count = len(failed_updates)

    if failed_updates:
        print(f"\n  X Failed to update {error_count} items")
        for item_id, error in failed_updates[:5]:  # Show first 5
            print(f"    - Item {item_id}: {error}")
        if len(failed_updates) > 5:
            print(f"    ... and {len(failed_updates) - 5} more failures")

    return success_count, error_count


async def update_duplicate_items_async(monday_api_key: str, duplicate_items: pd.DataFrame) -> tuple:
    """
    Update Monday items that are duplicates with a message indicating which item they duplicate.

    Args:
        monday_api_key: Monday.com API key
        duplicate_items: pandas DataFrame with columns: id, reason (and optionally board_id)

    Returns:
        Tuple of (success_count, error_count)
    """
    failed_updates = []

    # Filter to only items that have a reason (matched duplicates)
    items_to_update = duplicate_items[duplicate_items["reason"].notna()].copy()

    if len(items_to_update) == 0:
        return 0, 0

    async with httpx.AsyncClient(timeout=3000.0) as session:
        tasks = []
        for _, row in items_to_update.iterrows():
            item_id = str(row["id"])
            board_id = str(row.get("board_id", DEFAULT_MONDAY_BOARD_ID))
            reason_message = row["reason"]

            tasks.append(
                update_single_item_async(
                    monday_api_key,
                    board_id,
                    item_id,
                    reason_message,  # This goes to error_message param
                    "duplicate",  # This goes to error_type param
                    session,
                )
            )

        # Use tqdm for progress tracking
        pbar = tqdm(total=len(tasks), desc="Updating duplicate items")

        for coro in asyncio.as_completed(tasks):
            success, item_id, error = await coro
            pbar.update(1)

            if not success:
                failed_updates.append((item_id, error))

        pbar.close()

    success_count = len(items_to_update) - len(failed_updates)
    error_count = len(failed_updates)

    if failed_updates:
        print(f"\n  X Failed to update {error_count} duplicate items")
        for item_id, error in failed_updates[:5]:  # Show first 5
            print(f"    - Item {item_id}: {error}")
        if len(failed_updates) > 5:
            print(f"    ... and {len(failed_updates) - 5} more failures")

    return success_count, error_count


def update_monday_items_with_errors(environment: str, skip_fix_phase: bool = False):
    """
    Three-phase update: Clear fixed items, update broken items, and update duplicate items.

    Phase 1: Clear error column for items in tracking table (successfully processed)
    Phase 2: Update error column for items in DLQ (currently broken)
    Phase 3: Update error column for duplicate items (unprocessed items that match processed items)

    Args:
        environment: The environment (dev or prod)
        skip_fix_phase: If True, skip Phase 1 (clearing fixed items)
    """
    mode = "two-phase (skip fix)" if skip_fix_phase else "three-phase"
    print(f"Starting {mode} Monday error update for environment: {environment}")
    print("=" * 80)

    # Get table names
    table_names = get_table_names(environment)
    dlq_table = table_names["monday_download_dlq"]
    tracking_table = table_names["monday_tracking_table"]
    print(f"DLQ table: {dlq_table}")
    print(f"Tracking table: {tracking_table}")

    # Get Monday API key from secrets
    monday_api_key = dbutils.secrets.get("datn", "monday_api_key")
    print("Retrieved Monday API key from secrets")
    print()

    # ========================================================================
    # PHASE 1: Clear error column for fixed items (successfully processed)
    # ========================================================================
    if skip_fix_phase:
        print(">> PHASE 1: SKIPPED (--no-fix flag enabled)")
        print()
        clear_success = 0
        clear_errors = 0
    else:
        print("PHASE 1: Clearing errors for successfully processed items...")
        print("-" * 80)

        fixed_df = get_fixed_items(tracking_table)
        fixed_count = fixed_df.count()

        if fixed_count == 0:
            print("No fixed items found in tracking table")
            clear_success = 0
            clear_errors = 0
        else:
            print(f"Found {fixed_count} successfully processed items")
            fixed_items = fixed_df.collect()
            clear_success, clear_errors = asyncio.run(clear_monday_errors_async(monday_api_key, fixed_items))

        print(f"[OK] Phase 1 complete: {clear_success} cleared, {clear_errors} errors")
        print()

    # ========================================================================
    # PHASE 2: Update error column for broken items (currently in DLQ)
    # ========================================================================
    print("PHASE 2: Updating errors for currently broken items...")
    print("-" * 80)

    broken_df = get_currently_broken_files(dlq_table, tracking_table)
    broken_count = broken_df.count()

    if broken_count == 0:
        print("No currently broken files found (all files in DLQ have been successfully downloaded)")
        update_success = 0
        update_errors = 0
    else:
        print(f"Found {broken_count} currently broken files")
        error_items = broken_df.collect()
        update_success, update_errors = asyncio.run(update_monday_errors_async(monday_api_key, error_items))

    print(f"[OK] Phase 2 complete: {update_success} updated, {update_errors} errors")
    print()

    # ========================================================================
    # PHASE 3: Update error column for duplicate items
    # ========================================================================
    print("PHASE 3: Updating errors for duplicate items...")
    print("-" * 80)

    duplicates_df = get_duplicated_items()
    duplicate_count = len(duplicates_df[duplicates_df["reason"].notna()])

    if duplicate_count == 0:
        print("No duplicate items found")
        duplicate_success = 0
        duplicate_errors = 0
    else:
        print(f"Found {duplicate_count} duplicate items")
        duplicate_success, duplicate_errors = asyncio.run(update_duplicate_items_async(monday_api_key, duplicates_df))

    print(f"[OK] Phase 3 complete: {duplicate_success} updated, {duplicate_errors} errors")
    print()

    # Print summary
    print("=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    if skip_fix_phase:
        print("Phase 1 - Fixed items cleared:    SKIPPED")
    else:
        print(f"Phase 1 - Fixed items cleared:    {clear_success} success, {clear_errors} errors")
    print(f"Phase 2 - Broken items updated:   {update_success} success, {update_errors} errors")
    print(f"Phase 3 - Duplicate items updated: {duplicate_success} success, {duplicate_errors} errors")
    print(
        f"Total operations:                 {clear_success + update_success + duplicate_success} success, {
            clear_errors + update_errors + duplicate_errors
        } errors"
    )
    print("=" * 80)


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Update Monday.com board items with error messages from DLQ table")
    parser.add_argument(
        "--environment",
        required=True,
        choices=["dev", "prod"],
        help="Environment (dev/prod)",
    )
    parser.add_argument(
        "--no-fix",
        action="store_true",
        help="Skip Phase 1 (clearing fixed items), only update broken items with errors",
    )
    args = parser.parse_args()

    update_monday_items_with_errors(args.environment, skip_fix_phase=args.no_fix)
    print("Monday error update completed")


if __name__ == "__main__":
    main()
