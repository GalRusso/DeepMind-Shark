import argparse
import asyncio
import datetime
import io
import json
import traceback

import nest_asyncio
from aiogoogle.auth.creds import ServiceAccountCreds
from aiogoogle.client import Aiogoogle
from aiogoogle.excs import HTTPError
from constants import MONDAY_DLQ_SCHEMA, MONDAY_TRACKING_SCHEMA, ensure_tables_exist, get_table_names
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils, spark
from helpers import dir_path, safe_and_unique_filename, url_to_file_id
from pyspark.sql.functions import col
from pyspark.sql.types import Row
from tqdm import tqdm

nest_asyncio.apply()

EXPORT_PREFERENCES = {
    "application/vnd.google-apps.document": [
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/pdf",
        "text/html",
    ],
    "application/vnd.google-apps.spreadsheet": [
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/pdf",
    ],
    "application/vnd.google-apps.presentation": [
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "application/pdf",
    ],
    "application/vnd.google-apps.drawing": [
        "image/png",
    ],
}

# Supported MIME types and their file extensions
# Files with MIME types not in this dict will be rejected at download time
MIMETYPE_TO_EXTENSION = {
    "application/json": ".json",
    "application/pdf": ".pdf",
    "application/vnd.google-apps.document": ".docx",
    "application/vnd.google-apps.drawing": ".png",
    "application/vnd.google-apps.presentation": ".pptx",
    "application/vnd.google-apps.spreadsheet": ".xlsx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "image/png": ".png",
    "text/html": ".html",
    "text/markdown": ".md",
    "text/plain": ".txt",
}


# Fetch secret
service_account_json = dbutils.secrets.get("datn_af-kb", "google-service-account-json")
service_account_data = json.loads(service_account_json)
CREDS = ServiceAccountCreds(
    scopes=(
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ),
    **service_account_data,
)


async def fetch_gdrive_metadata_batch(file_ids: list[str]) -> list:
    async with Aiogoogle(service_account_creds=CREDS) as aiogoogle:
        drive_v3 = await aiogoogle.discover("drive", "v3")
        semaphore = asyncio.Semaphore(30)

        async def fetch_one(file_id):
            async with semaphore:
                meta_req = drive_v3.files.get(
                    fileId=file_id,
                    supportsAllDrives=True,
                    fields="*",
                )
                return await aiogoogle.as_service_account(meta_req)

        tasks = [fetch_one(fid) for fid in file_ids]
        return await asyncio.gather(*tasks, return_exceptions=True)


async def download_file_content(file_id: str, mime_type: str) -> tuple:
    async with Aiogoogle(service_account_creds=CREDS) as aiogoogle:
        drive_v3 = await aiogoogle.discover("drive", "v3")

        export_attempts = []
        last_exception = None
        response = None
        export_format_used = None

        if mime_type.startswith("application/vnd.google-apps."):
            export_preferences = EXPORT_PREFERENCES.get(mime_type, ["application/pdf"])

            for export_mime in export_preferences:
                try:
                    req = drive_v3.files.export(fileId=file_id, supportsAllDrives=True, mimeType=export_mime)
                    response = await aiogoogle.as_service_account(req)
                    export_format_used = export_mime
                    export_attempts.append({"format": export_mime, "success": True})
                    break
                except HTTPError as e:
                    export_attempts.append({"format": export_mime, "success": False, "error": str(e)})
                    last_exception = e
                    continue

            if response is None:
                raise last_exception if last_exception else Exception("All export formats failed")
        else:
            req = drive_v3.files.get(fileId=file_id, supportsAllDrives=True, alt="media")
            response = await aiogoogle.as_service_account(req)
            export_format_used = mime_type

        return response, export_format_used, export_attempts


workspace_client = WorkspaceClient()


def upload_file(file_path: str, content_io: io.BytesIO, overwrite: bool = True):
    """Wrapper to handle databricks-sdk version differences in upload parameter name."""
    try:
        workspace_client.files.upload(file_path=file_path, content=content_io, overwrite=overwrite)
    except TypeError as e:
        if "unexpected keyword argument 'content'" in str(e):
            workspace_client.files.upload(file_path=file_path, contents=content_io, overwrite=overwrite)
        else:
            raise


async def download_files_batch(items_with_metadata: list[tuple], environment: str, tracking_table: str, dlq_table: str):
    rows = []
    dlq_rows = []

    for item, gdrive_meta in items_with_metadata:
        board_id = item["board_id"]
        group_id = item["group_id"]
        item_id = item["item_id"]
        file_name = item["file_name"]
        gdrive_path = item["gdrive_path"]

        try:
            if isinstance(gdrive_meta, Exception):
                raise gdrive_meta

            file_id = url_to_file_id(gdrive_path)
            mime_type = gdrive_meta.get("mimeType")

            # Check if mime type is supported
            if mime_type not in MIMETYPE_TO_EXTENSION:
                raise ValueError(f"Unsupported file type: mime_type='{mime_type}'")

            content, export_format_used, export_attempts = await download_file_content(file_id, mime_type)

            full_metadata = {
                **gdrive_meta,
                "gdrive_path": gdrive_path,
                "board_id": board_id,
                "group_id": group_id,
                "item_id": item_id,
            }
            if export_format_used:
                full_metadata["export_format_used"] = export_format_used
            if export_attempts:
                full_metadata["export_attempts"] = export_attempts

            dest_dir = dir_path(environment)
            dbutils.fs.mkdirs(dest_dir)

            export_mime = export_format_used or mime_type
            filename = safe_and_unique_filename(
                gdrive_meta.get("name") or file_name, "unknown", export_mime, MIMETYPE_TO_EXTENSION
            )
            dest_path = f"{dest_dir}/{filename}"
            meta_path = f"{dest_path}.metadata.json"

            if isinstance(content, bytes):
                with io.BytesIO(content) as content_io:
                    upload_file(dest_path, content_io)
            elif isinstance(content, str):
                with io.BytesIO(content.encode("utf-8")) as content_io:
                    upload_file(dest_path, content_io)
            elif isinstance(content, (dict, list)):
                # JSON files are auto-parsed by aiogoogle, serialize them back
                json_str = json.dumps(content, indent=2)
                with io.BytesIO(json_str.encode("utf-8")) as content_io:
                    upload_file(dest_path, content_io)
            else:
                raise TypeError(
                    f"Unexpected content type: {type(content).__name__}. "
                    f"This may indicate an API error or unsupported response type."
                )

            with io.BytesIO(json.dumps(full_metadata).encode("utf-8")) as metadata_io:
                upload_file(meta_path, metadata_io)

            rows.append(
                Row(
                    board_id=board_id,
                    group_id=group_id,
                    item_id=item_id,
                    gdrive_path=gdrive_meta.get("webViewLink"),
                    original_path=dest_path,
                    metadata_path=meta_path,
                    created_at=datetime.datetime.now(),
                    file_revision_id=gdrive_meta.get("headRevisionId"),
                )
            )
        except Exception as e:
            print(f"Error downloading {file_name}: {e}, Traceback: {traceback.format_exc()}")
            dlq_rows.append(
                Row(
                    board_id=board_id,
                    group_id=group_id,
                    item_id=item_id,
                    file_name=file_name,
                    delivery_date=None,
                    gdrive_path=gdrive_path,
                    error_message=str(e),
                    error_type=type(e).__name__,
                    error_timestamp=datetime.datetime.now(),
                )
            )

    if rows:
        spark.createDataFrame(rows, schema=MONDAY_TRACKING_SCHEMA).write.mode("append").saveAsTable(tracking_table)

    if dlq_rows:
        spark.createDataFrame(dlq_rows, schema=MONDAY_DLQ_SCHEMA).write.mode("append").saveAsTable(dlq_table)


async def process_smart_downloads(
    environment: str,
    discovery_table: str,
    tracking_table: str,
    dlq_table: str,
    run_type: str = "incremental",
    backfill_dates: str | None = None,
):
    discovery_df = spark.table(discovery_table)
    tracking_df = spark.table(tracking_table)

    if run_type == "incremental":
        cutoff = datetime.datetime.now() - datetime.timedelta(days=2)
        discovery_df = discovery_df.where(col("created_at") >= cutoff)
        print(f"Incremental mode: filtering for created_at >= {cutoff}")
    elif run_type == "backfill":
        if not backfill_dates:
            raise ValueError("backfill_dates is required for backfill run_type")
        dates = [datetime.datetime.strptime(d.strip(), "%Y-%m-%d").date() for d in backfill_dates.split(",")]
        discovery_df = discovery_df.where(col("created_at").cast("date").isin(dates))
        print(f"Backfill mode: filtering for dates {dates}")
    else:
        print("Full refresh mode: processing all items")

    all_discovery_items = (
        discovery_df.select("board_id", "group_id", "item_id", "file_name", "gdrive_path")
        .where(col("gdrive_path").isNotNull())
        .collect()
    )

    all_discovery_items = [row.asDict() for row in all_discovery_items]

    if not all_discovery_items:
        print("No items found in discovery table")
        return

    print(f"Found {len(all_discovery_items)} items in discovery table")

    file_ids = [url_to_file_id(item["gdrive_path"]) for item in all_discovery_items]
    print(f"Fetching metadata for {len(file_ids)} files from Google Drive")
    gdrive_metadata_list = await fetch_gdrive_metadata_batch(file_ids)

    print("Filtering items that need download based on file_revision_id")
    tracking_map = {}
    for row in tracking_df.collect():
        key = (row.board_id, row.group_id, row.item_id)
        tracking_map[key] = row

    items_for_download = []
    for item, gdrive_meta in zip(all_discovery_items, gdrive_metadata_list, strict=True):
        if isinstance(gdrive_meta, Exception):
            items_for_download.append((item, gdrive_meta))
            continue

        key = (item["board_id"], item["group_id"], item["item_id"])
        tracking_record = tracking_map.get(key)

        if tracking_record is None:
            items_for_download.append((item, gdrive_meta))
            continue

        gdrive_revision = gdrive_meta.get("headRevisionId")
        tracked_revision = tracking_record.file_revision_id

        if gdrive_revision != tracked_revision:
            items_for_download.append((item, gdrive_meta))

    print(f"Items for download: {len(items_for_download)}")

    if items_for_download:
        chunks = [items_for_download[i : i + 100] for i in range(0, len(items_for_download), 100)]
        for chunk in tqdm(chunks, desc="Downloading files"):
            await download_files_batch(chunk, environment, tracking_table, dlq_table)


def main():
    parser = argparse.ArgumentParser(description="Download files from Google Drive based on Monday.com tracking")
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
        help="Type of data extraction: incremental (last 1 day), full_refresh (all data), or backfill (specific dates)",
    )
    parser.add_argument(
        "--backfill_dates",
        help="Comma-separated date strings in YYYY-MM-DD format for backfill run_type (required when run_type is backfill)",
    )
    args = parser.parse_args()

    if args.run_type == "backfill" and not args.backfill_dates:
        parser.error("--backfill_dates is required when --run_type is backfill")

    table_names = get_table_names(args.environment)
    ensure_tables_exist(args.environment)

    asyncio.run(
        process_smart_downloads(
            args.environment,
            table_names["monday_discovery_table"],
            table_names["monday_tracking_table"],
            table_names["monday_download_dlq"],
            args.run_type,
            args.backfill_dates,
        )
    )


if __name__ == "__main__":
    main()
