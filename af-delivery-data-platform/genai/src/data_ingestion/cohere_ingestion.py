"""
GenAI Cohere Data Ingestion Script

This script extracts Cohere data from Google Sheets and saves it to S3/Unity Catalog volumes.
It uses the refactored gspread2s3_utils module for the core ingestion logic.

Author: hoangl@activefence.com
Version: 1.0
"""

import sys
sys.path.append(".")
import argparse

from gspread2s3_utils import process_ingestion


def main():
    """Main entry point for GenAI Cohere data ingestion."""

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="GenAI Cohere Data Ingestion")
    parser.add_argument(
        "--environment",
        required=True,
        choices=["dev", "prod"],
        help="Environment (dev/prod)",
    )
    parser.add_argument(
        "--run_type",
        default="incremental",
        choices=["incremental", "full_refresh", "backfill"],
        help="Type of data extraction: incremental (today and yesterday), full_refresh (all data), or backfill (specific dates)",
    )
    parser.add_argument(
        "--backfill_dates",
        help="Comma-separated date strings in MM-DD-YYYY format for backfill run_type (required when run_type is backfill). Example: 01-15-2024,01-16-2024,01-17-2024",
    )
    args = parser.parse_args()

    # Validate backfill_dates is provided when run_type is backfill
    if args.run_type == "backfill" and not args.backfill_dates:
        parser.error("--backfill_dates is required when --run_type is backfill")

    # Hardcoded parameters for COhere data
    spreadsheet_id = "1tykiBILQWG3ZHIFAN_zgmDRDSgYU-mv8PxZnC-r4aN8"
    sheet_name = "Cohere"
    vertical = "genai"
    data_domain = "safety_and_security"
    file_name_prefix = "cohere"

    # Call the refactored ingestion function
    process_ingestion(
        environment=args.environment,
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        vertical=vertical,
        data_domain=data_domain,
        file_name_prefix=file_name_prefix,
        run_type=args.run_type,
        backfill_dates=args.backfill_dates,
        date_column="date_uploaded"
    )


if __name__ == "__main__":
    main()
