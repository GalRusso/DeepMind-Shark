import argparse
import asyncio
import json
from typing import Any

import nest_asyncio
from aiogoogle.auth.creds import ServiceAccountCreds
from aiogoogle.client import Aiogoogle
from constants import ensure_tables_exist, get_table_names
from databricks.sdk.runtime import dbutils, spark
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

nest_asyncio.apply()


def get_currently_broken_files(dlq_table: str, tracking_table: str) -> DataFrame:
    """
    Query currently broken files (files in DLQ that haven't been successfully downloaded).
    Returns the latest error per file.
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


def prepare_sheet_data(broken_files: list[dict], existing_sheet_data: list[list[Any]]) -> list[list[Any]]:
    """
    Upsert logic: merge currently broken files with existing sheet data.

    Args:
        broken_files: List of currently broken files from query
        existing_sheet_data: Existing rows from sheet (including header)

    Returns:
        Complete sheet data with headers and all rows (broken + fixed)
    """
    # Define header
    header = [
        "status",
        "board_id",
        "group_id",
        "item_id",
        "file_name",
        "delivery_date",
        "gdrive_path",
        "error_message",
        "error_type",
        "error_timestamp",
    ]

    # Build lookup dict for currently broken files by composite key
    broken_lookup = {}
    for file_data in broken_files:
        key = (file_data["board_id"], file_data["group_id"], file_data["item_id"])
        broken_lookup[key] = file_data

    # Process existing sheet data
    result_dict = {}

    # If sheet has data (beyond header), process it
    if len(existing_sheet_data) > 1:
        for row in existing_sheet_data[1:]:  # Skip header
            if len(row) < 4:  # Need at least status, board_id, group_id, item_id
                continue

            # Parse existing row (pad with None if missing columns)
            row_padded = row + [None] * (10 - len(row))
            board_id = row_padded[1]
            group_id = row_padded[2]
            item_id = row_padded[3]

            if not board_id or not group_id or not item_id:
                continue

            key = (board_id, group_id, item_id)

            # Check if this file is currently broken
            if key in broken_lookup:
                # Update with latest error info, mark as Broken
                file_data = broken_lookup[key]
                result_dict[key] = [
                    "Broken",
                    file_data["board_id"],
                    file_data["group_id"],
                    file_data["item_id"],
                    file_data["file_name"],
                    file_data["delivery_date"],
                    file_data["gdrive_path"],
                    file_data["error_message"],
                    file_data["error_type"],
                    str(file_data["error_timestamp"]) if file_data["error_timestamp"] else None,
                ]
                # Remove from broken_lookup since we've processed it
                del broken_lookup[key]
            else:
                # File was broken before but not anymore - mark as Fixed
                result_dict[key] = [
                    "Fixed",
                    row_padded[1],  # board_id
                    row_padded[2],  # group_id
                    row_padded[3],  # item_id
                    row_padded[4],  # file_name
                    row_padded[5],  # delivery_date
                    row_padded[6],  # gdrive_path
                    row_padded[7],  # error_message
                    row_padded[8],  # error_type
                    row_padded[9],  # error_timestamp
                ]

    # Add new broken files that weren't in the sheet before
    for key, file_data in broken_lookup.items():
        result_dict[key] = [
            "Broken",
            file_data["board_id"],
            file_data["group_id"],
            file_data["item_id"],
            file_data["file_name"],
            file_data["delivery_date"],
            file_data["gdrive_path"],
            file_data["error_message"],
            file_data["error_type"],
            str(file_data["error_timestamp"]) if file_data["error_timestamp"] else None,
        ]

    # Convert to list and sort by timestamp (most recent first)
    # Put Broken items first, then Fixed items, each sorted by timestamp desc
    result_rows = list(result_dict.values())
    broken_rows = [row for row in result_rows if row[0] == "Broken"]
    fixed_rows = [row for row in result_rows if row[0] == "Fixed"]

    # Sort each group by timestamp (index 9) descending
    broken_rows.sort(key=lambda x: x[9] if x[9] else "", reverse=True)
    fixed_rows.sort(key=lambda x: x[9] if x[9] else "", reverse=True)

    # Combine: header + broken + fixed
    return [header] + broken_rows + fixed_rows


async def export_to_sheet(
    broken_files: list[dict],
    spreadsheet_id: str,
    sheet_name: str = "Sheet1",
):
    """
    Export currently broken files to Google Sheet with upsert logic.

    Args:
        broken_files: List of currently broken files from DLQ
        spreadsheet_id: Google Sheet ID
        sheet_name: Sheet name/tab (default: Sheet1)
    """
    # Get credentials from Databricks secrets
    service_account_json = dbutils.secrets.get("datn_af-kb", "google-service-account-json-datn")
    service_account_data = json.loads(service_account_json)
    creds = ServiceAccountCreds(
        scopes=(
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/spreadsheets",
        ),
        **service_account_data,
    )

    async with Aiogoogle(service_account_creds=creds) as aiogoogle:
        sheets_v4 = await aiogoogle.discover("sheets", "v4")

        # Read existing sheet data
        range_name = f"{sheet_name}!A:J"  # All columns we use
        existing_data = []

        try:
            req = sheets_v4.spreadsheets.values.get(spreadsheetId=spreadsheet_id, range=range_name)
            response = await aiogoogle.as_service_account(req)
            existing_data = response.get("values", [])
            print(f"Read {len(existing_data)} existing rows from sheet")
        except Exception as e:
            print(f"Could not read existing data (sheet might be empty): {e}")
            existing_data = []

        # Prepare upserted data
        sheet_data = prepare_sheet_data(broken_files, existing_data)

        print(f"Prepared {len(sheet_data)} rows (including header) to write")

        # Clear the sheet first
        clear_req = sheets_v4.spreadsheets.values.clear(spreadsheetId=spreadsheet_id, range=range_name)
        await aiogoogle.as_service_account(clear_req)
        print("Cleared existing sheet data")

        # Write new data
        write_req = sheets_v4.spreadsheets.values.update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            json={"values": sheet_data},
        )
        write_response = await aiogoogle.as_service_account(write_req)
        print(f"Updated {write_response.get('updatedCells', 0)} cells in sheet")

        # Count broken vs fixed
        broken_count = sum(1 for row in sheet_data[1:] if row[0] == "Broken")
        fixed_count = sum(1 for row in sheet_data[1:] if row[0] == "Fixed")
        print(f"Sheet now contains: {broken_count} Broken, {fixed_count} Fixed files")


def main():
    parser = argparse.ArgumentParser(description="Export DLQ (currently broken files) to Google Sheet")
    parser.add_argument(
        "--environment",
        default="dev",
        choices=["dev", "prod"],
        help="Environment (dev/prod)",
    )
    parser.add_argument(
        "--spreadsheet-id",
        default="1xUOQB6It53Xw6vujS2t16XujPUJm6d841CATJE5b4M0",
        help="Google Sheet ID (from URL)",
    )
    parser.add_argument(
        "--sheet-name",
        default="Sheet1",
        help="Sheet name/tab (default: Sheet1)",
    )
    args = parser.parse_args()

    # Get table names and ensure tables exist
    table_names = get_table_names(args.environment)
    ensure_tables_exist(args.environment)

    print(f"Querying DLQ table: {table_names['monday_download_dlq']}")
    print(f"Tracking table: {table_names['monday_tracking_table']}")

    # Query currently broken files
    broken_df = get_currently_broken_files(
        table_names["monday_download_dlq"],
        table_names["monday_tracking_table"],
    )

    broken_count = broken_df.count()
    print(f"Found {broken_count} currently broken files")

    if broken_count == 0:
        print("No broken files found. Exiting.")
        # Still run export to mark any existing sheet items as "Fixed"
        broken_files = []
    else:
        # Convert to list of dicts
        broken_files = [row.asDict() for row in broken_df.collect()]

    # Export to Google Sheet
    print(f"Exporting to Google Sheet: {args.spreadsheet_id} (sheet: {args.sheet_name})")
    asyncio.run(
        export_to_sheet(
            broken_files,
            args.spreadsheet_id,
            args.sheet_name,
        )
    )

    print(f"Export complete!, view at https://docs.google.com/spreadsheets/d/{args.spreadsheet_id}")


if __name__ == "__main__":
    main()
