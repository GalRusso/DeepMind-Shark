"""
EWD Trends US Data Ingestion Script

This script extracts EWD Trends US data from Monday.com and saves it to S3/Unity Catalog volumes.
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
    """Main entry point for EWD Trends US ingestion."""

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="EWD Trends US Data Ingestion")
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

    # Hardcoded parameters for EWD Trends US
    board_id = "2308146097"  # US Trends board ID
    group_id = "topics"
    vertical = "dismis"
    data_domain = "trends"
    file_name_prefix = "ewd_trends_us"

    # Get Monday.com API key from Databricks secrets
    monday_api_key = dbutils.secrets.get(scope="hoangl", key="monday_api_key")

    # EWD Trends US specific columns
    include_columns = [
        "id",
        "name",
        "external_trend_id",
        "internal_date_reported",
        "geo",
        "date_published_link",
        "source_platform",
        "example_link",
        "screenshot",
        "screenshot_link_folder",
        "language_spread_primary",
        "trend_medium",
        "trend_name",
        "trend_description",
        "additional_notes",
        "platform_spread",
        "related_trends",
        "keywords",
        "potential_products_impacted",
        "reach_estimation",
        "risk_estimation",
        "send_spreadsheet",
        "msm_date",
        "msm_link",
        "analyst",
        "internal_content_tag",
        "client_feedback",
        "item_id",
        "automated_client_feedback_date",
        "trend_type",
        "content_tag",
        "risk_type",
        "external_date_reported",
        "creation_log",
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
