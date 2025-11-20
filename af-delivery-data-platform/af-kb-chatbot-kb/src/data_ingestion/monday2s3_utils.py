import json
import sys
from datetime import datetime
from typing import Literal

from databricks.sdk.runtime import dbutils

# Add shared library path
sys.path.append("../../../shared")
from libs import monday


def process_ingestion(
    environment: Literal["dev", "prod"],
    monday_api_key: str,
    board_id: str,
    group_id: str,
    vertical: str,
    data_domain: str,
    file_name_prefix: str,
    *,
    get_subitems: bool = False,
    include_columns: list[str] | None = None,
    exclude_columns: list[str] | None = None,
    run_type: str = "incremental",
    backfill_dates: str | None = None,
) -> str | None:
    """Monday.com to S3 Data Ingestion Script.

    Parameters
    ----------
    environment: Literal["dev", "prod"]
        Environment to run in (dev/prod)
    monday_api_key: str
        Monday.com API key
    board_id: str
        Monday.com board ID
    group_id: str
        Monday.com group ID
    vertical: str
        Data vertical (e.g., dismis)
    data_domain: str
        Data domain (e.g., trends)
    file_name_prefix: str
        Prefix for output file name
    get_subitems: bool, default False
        Whether to fetch subitems
    include_columns: list[str], optional
        List of columns to include
    exclude_columns: list[str], optional
        List of columns to exclude
    run_type: str, default "incremental"
        Type of data extraction: "incremental" (yesterday and today),
        "full_refresh" (all data), or "backfill" (specific dates)
    backfill_dates: str, optional
        Comma-separated date strings in YYYY-MM-DD format for backfill run_type
        Example: "2024-01-15,2024-01-16,2024-01-17"
    """

    # Validate mutually exclusive parameters
    if include_columns and exclude_columns:
        raise ValueError("include_columns and exclude_columns are mutually exclusive. Use only one or neither.")

    # Parse backfill_dates if provided and build filter_dates based on run_type
    filter_dates = None
    if run_type == "incremental":
        filter_dates = ["YESTERDAY", "TODAY"]
    elif run_type == "backfill" and backfill_dates:
        # Parse comma-separated dates
        parsed_dates = [date.strip() for date in backfill_dates.split(",")]
        # # Validate date format (basic validation)
        # for date in parsed_dates:
        #     if not (len(date) == 10 and date[4] == '-' and date[7] == '-'):
        #         raise ValueError(f"Invalid date format: {date}. Expected YYYY-MM-DD format")
        filter_dates = parsed_dates
    elif run_type == "full_refresh":
        filter_dates = ["PAST_DATETIME"]
    elif run_type == "backfill" and not backfill_dates:
        raise ValueError("backfill_dates is required when run_type is 'backfill'")

    print("Starting Monday.com to S3 ingestion process")
    print(
        f"Parameters: environment={environment}, board_id={board_id}, group_id={group_id}, \n"
        f"vertical={vertical}, data_domain={data_domain}, file_name_prefix={file_name_prefix}, "
        f"get_subitems={get_subitems}, \n"
        f"run_type={run_type}, backfill_dates={backfill_dates}, \n"
        f"include_columns={include_columns}, \n"
        f"exclude_columns={exclude_columns}"
    )

    # Fetch data from Monday.com
    print(f"Fetching data from Monday.com board {board_id}, group {group_id}")

    DATA_SOURCE = "monday"

    json_data = monday.get_board_items(
        api_key=monday_api_key,
        board_id=board_id,
        group_id=group_id,
        get_subitems=get_subitems,
        include_columns=include_columns,
        exclude_columns=exclude_columns,
        filter_dates=filter_dates,
    )
    print(f"Successfully fetched {len(json_data)} items from Monday.com")

    if len(json_data) == 0:
        print("No data found in Monday.com")
        return

    # Check if any include_columns are not in the data
    if include_columns:
        for col in include_columns:
            if col not in json_data[0]:
                print(f"Column {col} not found in the data")

    # Generate file paths
    now = datetime.now()

    # Create directory path with date partitioning
    dir_path = (
        f"/Volumes/af_delivery_{environment}/data_collection/"
        f"{vertical}/{data_domain}/{DATA_SOURCE}/"
        f"{now:%Y}/{now:%m}/{now:%d}"
    )

    # Generate timestamped file name
    file_name = f"{file_name_prefix}_{now:%Y%m%d_%H%M%S}.json"

    # Full volume path
    volume_path = f"{dir_path}/{file_name}"

    # Write data to volume
    # Create directory if it doesn't exist
    print(f"Creating directory: {dir_path}")
    dbutils.fs.mkdirs(dir_path)

    # Convert data to JSON string with proper formatting
    json_content = json.dumps(json_data, indent=2, ensure_ascii=False)

    # Write to volume
    print(f"Writing data to: {volume_path}")
    dbutils.fs.put(volume_path, json_content, overwrite=True)
    print(f"Successfully wrote {len(json_data)} items to {volume_path}")

    print("Monday.com to S3 ingestion completed successfully")
    return volume_path


if __name__ == "__main__":
    process_ingestion()
