import datetime
import hashlib
import json
import mimetypes
import os
import re

from constants import VERTICAL


def dir_path(environment: str):
    kind = "all"
    now = datetime.datetime.now()
    return f"/Volumes/af_delivery_{environment}/data_collection/{VERTICAL}/{kind}/monday/{now:%Y}/{now:%m}/{now:%d}"


def safe_and_unique_filename(name, default, mime_type=None, mime_type_to_extension: dict | None = None):
    """Create a safe filename from the given name or use default"""
    if mime_type_to_extension is None:
        mime_type_to_extension = {}
    timestamp = datetime.datetime.now().strftime("%H%M%S%f")
    if not name:
        return default + f"__{timestamp}"

    # Extract extension from original name
    name_part, extension = os.path.splitext(name)
    guessed_mimetype, _ = mimetypes.guess_type(name)
    
    # Keep original extension only if it matches the provided mime_type
    if extension and guessed_mimetype == mime_type:
        safe_name = re.sub(r"[^A-Za-z0-9_-]", "_", name_part)
    else:
        # Sanitize entire name and use mime_type for extension
        safe_name = re.sub(r"[^A-Za-z0-9_-]", "_", name)
        extension = mime_type_to_extension.get(mime_type, "") if mime_type else ""

    if not extension:
        raise ValueError(f"No extension found for {name=}, {mime_type=}")

    # Remove leading underscores/dots to avoid Spark treating files as hidden
    # Spark/Hadoop ignore files starting with _ or . as they're considered metadata
    safe_name = safe_name.lstrip("_.")
    
    # Ensure we still have a valid name after stripping
    if not safe_name:
        safe_name = "file"

    # Limit the length of the name to 200 characters
    # need 12 characters for the timestamp
    # some characters for the extension
    # need 15 characters for `.metadata.json`
    safe_name = safe_name[:200]
    unique_name = f"{safe_name}__{timestamp}{extension}"
    return unique_name


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


def compute_dict_hash(metadata_dict: dict) -> str:
    return hashlib.sha256(json.dumps(metadata_dict, sort_keys=True).encode()).hexdigest()


if __name__ == "__main__":
    print(
        safe_and_unique_filename(
            "[Full Report] Malware Threat - Hermes-Info Collector; Bell Mos (online.chattsvidios.bellmos.videomiss).docx",
            "test",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            {"application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx"},
        )
    )
    print(
        safe_and_unique_filename(
            "[Full Report] Malware Threat - Hermes-Info Collector; Bell Mos (online.chattsvidios.bellmos.videomiss).docx",
            "test2",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            {"application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx"},
        )
    )
    print(
        safe_and_unique_filename(
            "Malware Threat - Hermes-Info Collector; Bell Mos (online.chattsvidios.bellmos.videomiss)",
            "unknown",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            {"application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx"},
        )
    )

    print(
        safe_and_unique_filename(
            "Android Intel SDK - Quadrant.io",
            "unknown",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            {"application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx"},
        )
    )


    print(
        safe_and_unique_filename(
            "Multiple App Report - Archive.org",
            "unknown",
            "text/html",
            {"text/html": ".html"},
        )
    )