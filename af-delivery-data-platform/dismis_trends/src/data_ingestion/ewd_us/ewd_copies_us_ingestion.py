"""
EWD Copies US Data Ingestion Script

This script extracts EWD Copies US data from Monday.com and saves it to S3/Unity Catalog volumes.
It uses the refactored monday2s3_utils module for the core ingestion logic.

Author: hoangl@activefence.com
Version: 2.0
"""

import sys
sys.path.append("..")
sys.path.append("../../../../shared")
import argparse
from databricks.sdk.runtime import dbutils

from monday2s3_utils import process_ingestion


def main():
    """Main entry point for EWD Copies US ingestion."""

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="EWD Copies US Data Ingestion")
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
        help="Type of data extraction: incremental (yesterday and today), full_refresh (all data), or backfill (specific dates)",
    )
    parser.add_argument(
        "--backfill_dates",
        help="Comma-separated date strings in YYYY-MM-DD format for backfill run_type (required when run_type is backfill). Example: 2024-01-15,2024-01-16,2024-01-17",
    )
    args = parser.parse_args()

    # Validate backfill_dates is provided when run_type is backfill
    if args.run_type == "backfill" and not args.backfill_dates:
        parser.error("--backfill_dates is required when --run_type is backfill")

    # Hardcoded parameters for EWD Copies US
    board_id = "2307307551"  # US Copies board ID
    group_id = "topics"
    vertical = "dismis"
    data_domain = "trends"
    file_name_prefix = "ewd_copies_us"

    # Get Monday.com API key from Databricks secrets
    monday_api_key = dbutils.secrets.get(scope="hoangl", key="monday_api_key")

    # EWD Copies US specific columns (more comprehensive list for copies/artifacts)
    include_columns = [
        "name",
        "geo",
        "date_reported",
        "screenshot",
        "link",
        "link_s_platform",
        "online_status",
        "comment",
        "sent",
        "creation_log",
        "id",
        "ewd_trends_us",
        "analyst",
        "external_artifact_id",
        "external_trend_id",
        "last_updated",
    ]

    # Call the refactored ingestion function
    process_ingestion(
        environment=args.environment,
        monday_api_key=monday_api_key,
        board_id=board_id,
        group_id=group_id,
        vertical=vertical,
        data_domain=data_domain,
        file_name_prefix=file_name_prefix,
        include_columns=include_columns,
        run_type=args.run_type,
        backfill_dates=args.backfill_dates,
    )


if __name__ == "__main__":
    main()
