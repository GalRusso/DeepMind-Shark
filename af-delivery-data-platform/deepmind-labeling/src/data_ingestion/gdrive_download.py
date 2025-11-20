import asyncio
import datetime
import io
import json
import os
import re
from functools import lru_cache
from typing import Literal

import nest_asyncio
from aiogoogle.auth.creds import ServiceAccountCreds
from aiogoogle.client import Aiogoogle
from constants import ENVIRONMENT, FILES_TABLE, SOURCE, SUPPORTED_MAJOR_TYPES, TRACKING_SCHEMA, TRACKING_TABLE, VERTICAL
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils, spark
from pyspark.sql.functions import col
from pyspark.sql.types import Row
from tqdm import tqdm

nest_asyncio.apply()

# Note: This file doesn't directly import from libs, but uses shared constants


def dir_path(kind: Literal["image", "audio", "video"]):
    assert kind in SUPPORTED_MAJOR_TYPES
    now = datetime.datetime.now()
    return f"/Volumes/af_delivery_{ENVIRONMENT}/data_collection/labeling/{VERTICAL}/{kind}/{SOURCE}/{now:%Y}/{now:%m}/{now:%d}"


@lru_cache(maxsize=1000)
def mkdir(path):
    dbutils.fs.mkdirs(path)


def safe_and_unique_filename(name, default):
    """Create a safe filename from the given name or use default"""
    timestamp = datetime.datetime.now().strftime("%H%M%S%f")
    if not name:
        return default + f"__{timestamp}"
    safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    safe_name, extension = os.path.splitext(safe_name)
    unique_name = f"{safe_name}__{timestamp}{extension}"
    return unique_name[:255]


# Fetch secret
service_account_json = dbutils.secrets.get("pacman-keys", "google-service-account-test")
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


async def adownload_batch(file_ids):
    async with Aiogoogle(service_account_creds=CREDS) as aiogoogle:
        drive_v3 = await aiogoogle.discover("drive", "v3")
        semaphore = asyncio.Semaphore(30)

        async def inner_download(file_id):
            async with semaphore:
                meta_req = drive_v3.files.get(fileId=file_id, supportsAllDrives=True, fields="*")
                meta = await aiogoogle.as_service_account(meta_req)
                major = meta["mimeType"].split("/")[0]
                if major not in SUPPORTED_MAJOR_TYPES:
                    return None, meta
                else:
                    req = drive_v3.files.get(fileId=file_id, supportsAllDrives=True, alt="media")
                    response = await aiogoogle.as_service_account(req)
                return response, meta

        tasks = [inner_download(file_id) for file_id in file_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results


workspace_client = WorkspaceClient()


async def do_a_batch(sheet_ids, source_ids, file_names, gdrive_paths):
    results = await adownload_batch(source_ids)
    rows = []
    for result, sheet_id, source_id, file_name, gdrive_path in zip(
        results, sheet_ids, source_ids, file_names, gdrive_paths, strict=True
    ):
        try:
            if isinstance(result, Exception):
                raise result
            content, metadata = result

            view_url = metadata["webViewLink"]
            true_revision_id = metadata["headRevisionId"]
            major = metadata["mimeType"].split("/")[0]

            if major not in SUPPORTED_MAJOR_TYPES and content is None:
                rows.append(
                    Row(
                        sheet_id=sheet_id,
                        source_id=source_id,
                        revision_id=true_revision_id,
                        gdrive_path=view_url,
                        original_path=None,
                        metadata_path=None,
                        error="[Known issue] Unsupported file type",
                        ts_downloaded=datetime.datetime.now(),
                    )
                )
                continue

            # enrich metadata with gdrive_path
            metadata["gdrive_path"] = gdrive_path

            dest_dir = dir_path(major)
            mkdir(dest_dir)
            filename = safe_and_unique_filename(file_name or metadata["name"], "unknown")
            dest_path = f"{dest_dir}/{filename}"
            meta_path = f"{dest_path}.metadata.json"

            # Write using WorkspaceClient
            with io.BytesIO(content) as content_io:
                workspace_client.files.upload(dest_path, content_io, overwrite=True)

            with io.BytesIO(json.dumps(metadata).encode("utf-8")) as metadata_io:
                workspace_client.files.upload(meta_path, metadata_io, overwrite=True)

            rows.append(
                Row(
                    sheet_id=sheet_id,
                    source_id=source_id,
                    revision_id=true_revision_id,
                    gdrive_path=view_url,
                    original_path=dest_path,
                    metadata_path=meta_path,
                    error=None,
                    ts_downloaded=datetime.datetime.now(),
                )
            )
        except Exception as e:
            print(f"Error downloading {file_name}: {e}")

    # Append to tracking table
    spark.createDataFrame(rows, schema=TRACKING_SCHEMA).write.mode("append").saveAsTable(TRACKING_TABLE)


async def main(limit: int | None = None):
    files_df = spark.table(FILES_TABLE)
    tracking_df = spark.table(TRACKING_TABLE)
    pending_df = (
        files_df.alias("discovery")
        .join(
            tracking_df.alias("tracking"),
            on=(col("discovery.source_id") == col("tracking.source_id"))
            & (col("discovery.sheet_id") == col("tracking.sheet_id")),
            how="left_anti",
        )
        .select("sheet_id", "source_id", "source_url", "file_title")
    )
    if limit:
        pending_df = pending_df.orderBy("sheet_id", "source_id").limit(limit)
    pending_list = [row.asDict() for row in pending_df.collect()]

    # devide pending_list into chunks of 100
    batch_size = 100
    chunks = [pending_list[i : i + batch_size] for i in range(0, len(pending_list), batch_size)]
    for chunk in tqdm(chunks, desc="Downloading batches of files"):
        sheet_ids = [item["sheet_id"] for item in chunk]
        source_ids = [item["source_id"] for item in chunk]
        file_names = [item["file_title"] for item in chunk]
        gdrive_paths = [item["source_url"] for item in chunk]
        try:
            await do_a_batch(sheet_ids, source_ids, file_names, gdrive_paths)
        except Exception as e:
            print(f"Error downloading chunk: {e}")
            raise e


if __name__ == "__main__":
    asyncio.run(main())
