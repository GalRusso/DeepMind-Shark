import argparse
import datetime
import hashlib
import io
import json
import re
import sys
import traceback
from typing import Literal

from constants import (
    DOWNLOAD_DLQ_SCHEMA,
    DOWNLOAD_DLQ_TABLE,
    ENVIRONMENT,
    SOURCE,
    SUPPORTED_MAJOR_TYPES,
    TRACKING_SCHEMA,
    TRACKING_TABLE,
    VERTICAL,
)
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils, spark
from pyspark.sql.types import Row

# Add shared library to path (works in both local and Databricks execution)
import os
_current_dir = os.getcwd()
if '/Workspace/' in _current_dir:
    # Running in Databricks - use workspace path
    sys.path.insert(0, '/Workspace/Users/asafso@activefence.com/.bundle/deepmind-labeling/dev/files/shared')
else:
    # Running locally - use relative path
    _script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else _current_dir
    sys.path.insert(0, os.path.abspath(os.path.join(_script_dir, "../../shared")))

from libs.google_drive import cache_file, get_file_metadata

# %% arguments
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--source_url", default="")
parser.add_argument("--file_title", default="")
parser.add_argument("--file_hash", default="")

# Use parse_known_args to avoid errors if Databricks injects extra args
args, _ = parser.parse_known_args()

if not args.source_url:
    raise ValueError("source_url is required", args)

source_url = args.source_url
file_title = args.file_title
file_hash = args.file_hash


def dir_path(kind: Literal["image", "audio", "video"]):
    assert kind in SUPPORTED_MAJOR_TYPES
    now = datetime.datetime.now()
    return f"/Volumes/af_delivery_{ENVIRONMENT}/data_collection/labeling/{VERTICAL}/{kind}/{SOURCE}/{now:%Y}/{now:%m}/{now:%d}"


# %%
def safe_filename(name, default):
    """Create a safe filename from the given name or use default"""
    if not name:
        return default
    return re.sub(r"[^A-Za-z0-9._-]", "_", name)[:255]


# Fetch secret
service_account_json = dbutils.secrets.get("pacman-keys", "google-service-account-test")
service_account = json.loads(service_account_json)

# Get metadata
metadata = get_file_metadata(service_account, source_url, fields="id,name,mimeType")
major = metadata["mimeType"].split("/")[0]

if major not in SUPPORTED_MAJOR_TYPES:
    error_message = f"[Known issue] Unsupported file type: {major}"
    result = Row(
        source_url=source_url,
        file_hash=file_hash,
        drive_file_id=None,
        error_message=error_message,
        created_at=datetime.datetime.now(),
    )

    spark.createDataFrame([result], schema=DOWNLOAD_DLQ_SCHEMA).write.mode("append").saveAsTable(DOWNLOAD_DLQ_TABLE)
    exit(0)

try:
    content = cache_file(service_account, source_url)
    downloaded_hash = hashlib.md5(content).hexdigest()
except Exception as e:
    tb_str = traceback.format_exc()
    error_message = f"Error caching file: {e} {tb_str[:1000]}"
    result = Row(
        source_url=source_url,
        file_hash=file_hash,
        drive_file_id=None,
        error_message=error_message,
        created_at=datetime.datetime.now(),
    )

    spark.createDataFrame([result], schema=DOWNLOAD_DLQ_SCHEMA).write.mode("append").saveAsTable(DOWNLOAD_DLQ_TABLE)
    exit(0)

if downloaded_hash != file_hash:
    error_message = "[Known issue] Hash mismatch"
    result = Row(
        source_url=source_url,
        file_hash=file_hash,
        drive_file_id=metadata["id"],
        created_at=datetime.datetime.now(),
        error_message=error_message,
    )
    spark.createDataFrame([result], schema=DOWNLOAD_DLQ_SCHEMA).write.mode("append").saveAsTable(DOWNLOAD_DLQ_TABLE)
else:
    dest_path = dir_path(major)
    filename = safe_filename(file_title or metadata["name"], "unknown")
    dest_path = f"{dest_path}/{filename}"

    # Write using WorkspaceClient
    client = WorkspaceClient()
    with io.BytesIO(content) as content_io:
        client.files.upload(dest_path, content_io, overwrite=True)

    result = Row(
        source_url=source_url,
        file_hash=file_hash,
        drive_file_id=metadata["id"],
        dest_path=dest_path,
        ingested_at=datetime.datetime.now(),
    )
    spark.createDataFrame([result], schema=TRACKING_SCHEMA).write.mode("append").saveAsTable(TRACKING_TABLE)
