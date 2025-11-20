import argparse
import datetime
import json

from constants import MONDAY_DISCOVERY_SCHEMA, ensure_tables_exist, get_table_names
from databricks.sdk.runtime import dbutils, spark
from delta.tables import DeltaTable
from monday2s3_utils import process_ingestion
from pyspark.sql.types import Row


def main():
    parser = argparse.ArgumentParser(description="EWD Copies APAC Data Ingestion")
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

    # Get table names and ensure tables exist
    table_names = get_table_names(args.environment)
    ensure_tables_exist(args.environment)

    # Hardcoded parameters Ops & Intel Reports
    board_id = "18221691675"
    group_id = "group_mkwzb81w"
    vertical = "kb_unstructured"
    data_domain = "finalized_documents"  # vs on-going_documents
    file_name_prefix = "monday"

    # Get Monday.com API key from Databricks secrets
    monday_api_key = dbutils.secrets.get(scope="datn", key="monday_api_key")

    exclude_columns = [
        "make_error_message",  # [Make] Error Message, this is used by shani
    ]

    # Call the refactored ingestion function
    json_file = process_ingestion(
        environment=args.environment,
        monday_api_key=monday_api_key,
        board_id=board_id,
        group_id=group_id,
        vertical=vertical,
        data_domain=data_domain,
        file_name_prefix=file_name_prefix,
        exclude_columns=exclude_columns,
        run_type=args.run_type,
        backfill_dates=args.backfill_dates,
    )
    if not json_file:
        print("No data found in Monday.com")
        return
    with open(json_file) as f:
        json_data = json.load(f)

    """
    [{'name': 'Voi Trade Alert May 16, 2022 @Tung9388888',
    'id': '18252843322',
    'activemind_status': 'On Hold',
    'client': 'Flash Cyber',
    'team': 'CTI',
    'delivery_date': '2022-05-26',
    'owner': None,
    'bug_number': None,
    'final_report_file': None,
    'final_report_url': 'https://drive.google.com/file/d/1mGONdvULA8ZfaenPK5s6UIyA8jDH_o1z/view?usp=drivesdk',
    'rag_kb_url': 'https://drive.google.com/file/d/150cSMyvOUv3on8aKn74ne6W7kqYzRU_i/view?usp=drivesdk',
    'original_item': ['Voi Trade Alert May 16, 2022 @Tung9388888'],
    'match': 'Done',
    'type': 'VOI Trade Alert'},
    ]"""

    rows = []
    for item in json_data:
        item["board_id"] = board_id
        rows.append(
            Row(
                board_id=board_id,
                group_id=group_id,
                item_id=item.get("id"),
                file_name=item.get("name"),
                gdrive_path=item.get("rag_kb_url"),
                created_at=datetime.datetime.now(datetime.UTC),
                metadata=json.dumps(item),
            )
        )
    df = spark.createDataFrame(rows, schema=MONDAY_DISCOVERY_SCHEMA).dropDuplicates(["board_id", "group_id", "item_id"])
    delta_table = DeltaTable.forName(spark, table_names["monday_discovery_table"])
    (
        delta_table.alias("t")
        .merge(
            source=df.alias("s"),
            condition="t.board_id == s.board_id AND t.group_id = s.group_id AND t.item_id = s.item_id",
        )
        .whenMatchedUpdate(
            set={
                "file_name": "s.file_name",
                "gdrive_path": "s.gdrive_path",
                "metadata": "s.metadata",
                "created_at": "s.created_at",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


if __name__ == "__main__":
    main()
