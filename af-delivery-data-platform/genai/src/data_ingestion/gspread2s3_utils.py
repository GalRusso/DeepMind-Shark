"""
Google Sheets to S3 Data Ingestion Script

This script extracts data from Google Sheets and saves it to S3/Unity Catalog volumes in a structured format for downstream processing.
Supports incremental, full refresh, and backfill ingestion modes.

Author: hoangl@activefence.com
Version: 1.0
"""

import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Literal

from databricks.sdk.runtime import dbutils
import gspread

def process_ingestion(
    environment: Literal["dev", "prod"],
    spreadsheet_id: str,
    sheet_name: str,
    vertical: str,
    data_domain: str,
    file_name_prefix: str,
    *,
    run_type: str = "incremental",
    backfill_dates: str = None,
    date_column: str = "date_uploaded"
):
    """Google Sheets to S3 Data Ingestion Script.
    
    Parameters
    ----------
    environment: Literal["dev", "prod"]
        Environment to run in (dev/prod)
    spreadsheet_id: str
        Google Spreadsheet ID
    sheet_name: str
        Name of the sheet/tab to extract data from
    vertical: str
        Data vertical (e.g., genai)
    data_domain: str
        Data domain (e.g., safety_and_security)
    file_name_prefix: str
        Prefix for output file name
    run_type: str, default "incremental"
        Type of data extraction: "incremental" (today and yesterday), 
        "full_refresh" (all data), or "backfill" (specific dates)
    backfill_dates: str, optional
        Comma-separated date strings in MM-DD-YYYY format for backfill run_type
        Example: "01-15-2024,01-16-2024,01-17-2024"
    date_column: str, default "date_uploaded"
        Column name to use for date filtering
    """
    
    print("Starting Google Sheets to S3 ingestion process")
    print(
        f"Parameters: environment={environment}, spreadsheet_id={spreadsheet_id}, "
        f"sheet_name={sheet_name}, vertical={vertical}, data_domain={data_domain}, "
        f"file_name_prefix={file_name_prefix}, run_type={run_type}, "
        f"backfill_dates={backfill_dates}, date_column={date_column}"
    )

    # Fetch data from Google Sheets
    print(f"Fetching data from Google Sheets: {spreadsheet_id}, sheet: {sheet_name}")
    
    try:
        client = gspread.service_account_from_dict(json.loads(dbutils.secrets.get(scope="hoangl", key="gdrive-sa")))
        ss = client.open_by_key(spreadsheet_id)
        df = pd.DataFrame.from_records(ss.worksheet(sheet_name).get_all_records())
        print(f"Successfully fetched data from Google Sheets")
    except Exception as e:
        print(f"Error fetching data from Google Sheets: {str(e)}")
        raise

    if df.empty:
        print("No data found in Google Sheets")
        print("Exiting...")
        return

    # Apply date filtering based on run_type
    filtered_df = df.copy()
    
    if run_type == "incremental":
        # Get today and yesterday data
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        
        if date_column in df.columns:
            # Convert date column to datetime for comparison
            df[date_column] = pd.to_datetime(df[date_column], format="%m/%d/%Y", errors='coerce')
            filtered_df = df[
                (df[date_column].dt.date == today) | 
                (df[date_column].dt.date == yesterday)
            ]
        else:
            print(f"Warning: Date column '{date_column}' not found. Using all data.")
            
    elif run_type == "backfill" and backfill_dates:
        # Parse comma-separated dates
        parsed_dates = [datetime.strptime(date.strip(), "%m/%d/%Y").date() 
                       for date in backfill_dates.split(",")]
        
        if date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column], format="%m/%d/%Y", errors='coerce')
            filtered_df = df[df[date_column].dt.date.isin(parsed_dates)]
        else:
            print(f"Warning: Date column '{date_column}' not found. Using all data.")
            
    elif run_type == "full_refresh":
        print(f"Using all {len(filtered_df)} rows for full refresh")
        
    elif run_type == "backfill" and not backfill_dates:
        raise ValueError("backfill_dates is required when run_type is 'backfill'")

    if filtered_df.empty:
        print("No new data found after filtering")
        print("Exiting...")
        return

    # Generate file paths
    now = datetime.now()

    # Create directory path with date partitioning
    DATA_SOURCE = "gsheets"
    dir_path = (
        f"/Volumes/af_delivery_{environment}/data_collection/"
        f"{vertical}/{data_domain}/{DATA_SOURCE}/"
        f"{now:%Y}/{now:%m}/{now:%d}"
    )

    # Generate timestamped file name
    file_name = f"{file_name_prefix}_{now:%Y%m%d_%H%M%S}.csv"

    # Full volume path
    volume_path = f"{dir_path}/{file_name}"

    # Write data to volume
    # Create directory if it doesn't exist
    print(f"Creating directory: {dir_path}")
    dbutils.fs.mkdirs(dir_path)

    # Convert data to CSV string with proper formatting
    csv_content = filtered_df.to_csv(index=False, quoting=1, date_format="%m/%d/%Y")

    # Write to volume
    print(f"Writing data to: {volume_path}")
    dbutils.fs.put(volume_path, csv_content, overwrite=True)
    print(f"Successfully wrote {len(filtered_df)} items to {volume_path}")

    print("Google Sheets to S3 ingestion completed successfully")
    