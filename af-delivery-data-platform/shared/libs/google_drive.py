import asyncio
import io
import json
import re
import traceback
from collections.abc import Mapping
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Literal

from aiogoogle.auth.creds import ServiceAccountCreds
from aiogoogle.client import Aiogoogle
from aiogoogle.excs import HTTPError
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


def url_to_file_id(drive_url: str):
    """
    Extract the file ID from a Google Drive URL.

    Args:
        drive_url (str): The URL of the Google Drive file.

    Returns:
        str: The extracted file ID from the URL.

    Raises:
        ValueError: If the file ID cannot be extracted from the URL.
    """
    match = re.search(r"/d/([a-zA-Z0-9_-]+)", drive_url)
    if not match:
        raise ValueError(f"Could not extract file id from drive url {drive_url}")
    return match.group(1)


def get_service(
    service_account_data: str | dict,
    purpose: Literal["download_file", "get_metadata"] = "download_file",
):
    """
    Create a Google Drive API service client using the provided service account credentials.

    Args:
        service_account_data (str | dict): Service account credentials as a JSON string or dictionary.

    Returns:
        googleapiclient.discovery.Resource: Google Drive API service client.
    """
    if isinstance(service_account_data, str):
        service_account_data = json.loads(service_account_data)

    match purpose:
        case "download_file":
            scopes = [
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/drive.file",
                "https://www.googleapis.com/auth/drive.readonly",
            ]
        case "get_metadata":
            scopes = [
                "https://www.googleapis.com/auth/drive.metadata.readonly",
            ]
        case _:
            raise ValueError(f"Invalid purpose: {purpose}")

    creds = service_account.Credentials.from_service_account_info(
        service_account_data, scopes=scopes
    )
    return build("drive", "v3", credentials=creds)


def cache_file(service_account_data: dict, drive_url: str) -> bytes:
    """
    Download a file from Google Drive and return its content as bytes.

    Args:
        service_account_data (str | dict): Service account credentials as a JSON string or dictionary.
        drive_url (str): The URL of the Google Drive file to download.

    Returns:
        bytes: The content of the downloaded file.
    """

    file_id = url_to_file_id(drive_url)
    return download_file(service_account_data, file_id)


def download_file(service_account_data: dict, file_id: str) -> bytes:
    """
    Download a file from Google Drive and return its content as bytes.

    Args:
        service_account_data (str | dict): Service account credentials as a JSON string or dictionary.
        drive_url (str): The URL of the Google Drive file to download.

    Returns:
        bytes: The content of the downloaded file.
    """

    service = get_service(service_account_data)

    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    return fh.getvalue()


def download_file_revision(
    service_account_data: dict, file_id: str, revision_id: str
) -> bytes:
    """
    Download a file from Google Drive and return its content as bytes.

    Args:
        service_account_data (str | dict): Service account credentials as a JSON string or dictionary.
        drive_url (str): The URL of the Google Drive file to download.

    Returns:
        bytes: The content of the downloaded file.
    """

    service = get_service(service_account_data)

    request = service.revisions().get(
        fileId=file_id, revisionId=revision_id, alt="media"
    )
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    return fh.getvalue()


def get_file_metadata(
    service_account_data: dict, drive_url: str, fields: str = "*"
) -> dict:
    """
    Get metadata for a file without downloading its content.

    Args:
        service_account_data (str | dict): Service account credentials as a JSON string or dictionary.
        drive_url (str): Google Drive URL of the file.
        fields (str, optional): Comma-separated list of fields to return, or "*" for all fields. Defaults to "*".
            Examples: "id,name,mimeType" or "id,name,mimeType,size,createdTime"

    Returns:
        dict: File metadata with requested fields.
    """
    service = get_service(service_account_data, purpose="get_metadata")
    file_id = url_to_file_id(drive_url)
    return (
        service.files()
        .get(fileId=file_id, supportsAllDrives=True, fields=fields)
        .execute()
    )


@asynccontextmanager
async def aiogoogle_client(service_account_data: dict):
    _creds = ServiceAccountCreds(
        scopes=[
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive.readonly",
            "https://www.googleapis.com/auth/drive.metadata.readonly",
        ],
        **service_account_data,
    )

    async with Aiogoogle(service_account_creds=_creds) as aiogoogle:
        yield aiogoogle


async def aget_file_metadata(
    service_account_data: dict, drive_urls: list[str], fields: str = "*"
) -> list[dict[str, Any]]:
    """
    Asynchronously retrieve metadata for one or more Google Drive files without downloading their content.

    Args:
        service_account_data (dict): Service account credentials as a dictionary.
        drive_urls (list[str]): List of Google Drive file URLs.
        fields (str, optional): Comma-separated list of fields to return, or "*" for all fields. Defaults to "*".
            Examples: "id,name,mimeType" or "id,name,mimeType,size,createdTime"

    Returns:
        list[dict[str, Any]]: List of metadata dictionaries for each file, containing the requested fields.

    Raises:
        ValueError: If a file ID cannot be extracted from any of the provided URLs.
    """
    file_ids = [url_to_file_id(drive_url) for drive_url in drive_urls]

    async with aiogoogle_client(service_account_data) as aiogoogle:
        drive_v3 = await aiogoogle.discover("drive", "v3")

        tasks = [
            aiogoogle.as_service_account(
                drive_v3.files.get(
                    fileId=file_id, supportsAllDrives=True, fields=fields
                )
            )
            for file_id in file_ids
        ]
        results = await asyncio.gather(*tasks)

        return results


async def adownload_files(
    service_account_data: dict, drive_urls: list[str], meta_fields: str | None = None
) -> AsyncGenerator[dict[str, Any], None]:
    """
    Asynchronously download contents of multiple Google Drive files using MediaDownload for streaming.
    Args:
        drive_urls: List of Google Drive share URLs.
    Returns:
        list[bytes]: List of file contents as bytes, in the order of input URLs.

    Raises:
        Exception: If any download fails.
    """
    file_ids = [url_to_file_id(drive_url) for drive_url in drive_urls]

    async with aiogoogle_client(service_account_data) as aiogoogle:
        drive_v3 = await aiogoogle.discover("drive", "v3")
        for i, file_id in enumerate(file_ids):
            try:
                req = drive_v3.files.get(
                    fileId=file_id, supportsAllDrives=True, alt="media"
                )
                response = await aiogoogle.as_service_account(req)
                meta = None
                if meta_fields is not None:
                    meta_req = drive_v3.files.get(
                        fileId=file_id, supportsAllDrives=True, fields=meta_fields
                    )
                    meta = await aiogoogle.as_service_account(meta_req)
                yield {"index": i, "content": response, "error": None, "meta": meta}
            except Exception as e:
                trace = traceback.format_exc(limit=2)
                trace_short = trace.replace("\n", " ")[:500]
                error_msg = f"Batch error: {e!s} | Trace: {trace_short}"
                yield {"index": i, "content": None, "error": error_msg, "meta": None}


async def aget_latest_revision(
    service_account_data: dict, file_ids: list[str]
) -> list[str | None]:
    """Return the last revision for *file_id* (or None if none exist).

    return example
    {'kind': 'drive#revision', 'id': '0BxGS5ytoH5jEVm9XbThSV0dKayt1azVOS0ZpL0lvRWRiTDg0PQ', 'mimeType': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'modifiedTime': '2023-10-14T17:29:48.000Z'}

    """
    semaphore = asyncio.Semaphore(30)
    async with aiogoogle_client(service_account_data) as aiogoogle:
        drive_v3 = await aiogoogle.discover("drive", "v3")

        async def inner_get_latest_revision(file_id: str) -> str | None:
            async with semaphore:
                try:
                    res = drive_v3.revisions.list(fileId=file_id, pageSize=1)
                    result = await aiogoogle.as_service_account(res)
                    return result.get("revisions", [])[-1]["id"]
                except HTTPError:
                    return None

        tasks = [inner_get_latest_revision(file_id) for file_id in file_ids]
        results = await asyncio.gather(*tasks)
        return results


async def alist_file_recursive(
    service_account_data: dict,
    folder_id: str,
    *,
    name_prefix: str | None = None,
    page_size: int = 1000,
    path_prefix: str | None = None,
    exclude_folder_ids: list[str] | None = None,
) -> list[Mapping]:
    """Return **all non-folder** descendants of *folder_id* using true recursion.

    This public method recurses into itself rather than an inner helper so
    the call-stack depth directly mirrors Drive folder depth.
    Each item is enriched with a ``path`` field containing the full relative
    path from the initial root folder.
    """

    if path_prefix is None:
        path_prefix = "."

    semaphore = asyncio.Semaphore(30)

    folder_id_queue = asyncio.Queue()
    output_queue = asyncio.Queue()

    async with aiogoogle_client(service_account_data) as aiogoogle:
        drive_v3 = await aiogoogle.discover("drive", "v3")

        async def fetch_forever() -> None:
            while True:
                folder_id, _path_prefix = await folder_id_queue.get()
                if folder_id is None:  # sentinel
                    folder_id_queue.task_done()
                    return

                # Establish human-readable path prefix at the top-level call

                query = f"'{folder_id}' in parents and trashed = false"
                if name_prefix:
                    query += f" and name contains '{name_prefix}'"

                # pagination loop
                page_token = None
                while True:
                    async with semaphore:  # rate limit
                        req = drive_v3.files.list(
                            q=query,
                            pageSize=page_size,
                            pageToken=page_token,
                            fields="nextPageToken, files(id,name,mimeType,modifiedTime)",
                            supportsAllDrives=True,
                            includeItemsFromAllDrives=True,
                        )
                        resp = await aiogoogle.as_service_account(req)

                        for item in resp.get("files", []):
                            # strict starts-with filter
                            if name_prefix and not item.get("name", "").startswith(
                                name_prefix
                            ):
                                continue
                            if exclude_folder_ids and item["id"] in exclude_folder_ids:
                                continue

                            item_path = f"{_path_prefix}/{item['name']}"

                            if item.get("mimeType") == _FOLDER_MIME:
                                await folder_id_queue.put((item["id"], item_path))
                            else:
                                item["path"] = item_path
                                await output_queue.put(item)

                        page_token = resp.get("nextPageToken")
                        if not page_token:
                            break

                folder_id_queue.task_done()

        # Create 30 worker tasks to process the queue concurrently.
        tasks = []
        for _ in range(30):
            task = asyncio.create_task(fetch_forever())
            tasks.append(task)

        await folder_id_queue.put((folder_id, path_prefix))
        await folder_id_queue.join()
        for _ in range(30):  # send sentinel to stop worker tasks
            await folder_id_queue.put((None, None))
        await folder_id_queue.join()
        items = []
        while not output_queue.empty():
            items.append(await output_queue.get())
            output_queue.task_done()
        await output_queue.join()
        return items
