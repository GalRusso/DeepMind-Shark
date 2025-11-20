import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build


def _col_to_a1(n: int) -> str:
    """
    Convert a 1-based column index to its corresponding A1 notation (e.g., 1 -> 'A', 27 -> 'AA').

    Args:
        n (int): 1-based column index.

    Returns:
        str: The column label in A1 notation.
    """
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def get_sheet_service(service_account_data: dict, scopes: list[str] | None = None) -> build:
    if scopes is None:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    creds = service_account.Credentials.from_service_account_info(
        service_account_data, scopes=scopes
    )

    return build("sheets", "v4", credentials=creds)

def get_sheet_metadata(service_account_data: dict, spreadsheet_id: str) -> dict:
    """
    Retrieve metadata for a Google Spreadsheet.

    Args:
        service_account_data (dict): Service account credentials as a dictionary.
        spreadsheet_id (str): The ID of the Google Spreadsheet.

    Returns:
        dict: The metadata of the spreadsheet as returned by the Google Sheets API.
    """
    svc = get_sheet_service(service_account_data)
    meta = svc.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        includeGridData=False,
    ).execute()
    if meta is None:
        raise ValueError("API returned None for metadata")
    return meta


def get_grid_limits(
    service_account_data: dict, spreadsheet_id: str, sheet_name: str
) -> tuple[int, int]:
    """
    Get the number of rows and columns in a specific sheet within a spreadsheet.

    Args:
        service_account_data (dict): Service account credentials as a dictionary.
        spreadsheet_id (str): The ID of the Google Spreadsheet.
        sheet_name (str): The name of the sheet to inspect.

    Returns:
        tuple[int, int]: A tuple containing (row_count, col_count).

    Raises:
        RuntimeError: If the specified sheet is not found in the spreadsheet.
    """
    meta = get_sheet_metadata(service_account_data, spreadsheet_id)

    for sh in meta.get("sheets", []):
        props = sh.get("properties", {})
        if props.get("title") == sheet_name:
            grid = props.get("gridProperties", {})
            row_count = int(grid.get("rowCount", 1000000))
            col_count = int(grid.get("columnCount", 26))
            return row_count, col_count

    raise RuntimeError(f"Sheet '{sheet_name}' not found in spreadsheet metadata.")


def get_sheet_values(
    service_account_data: dict, spreadsheet_id: str, sheet_name: str
) -> pd.DataFrame:
    """
    Retrieve all values from a Google Sheet as a pandas DataFrame, handling large sheets in chunks.

    Args:
        service_account_data (dict): Service account credentials as a dictionary.
        spreadsheet_id (str): The ID of the Google Spreadsheet.
        sheet_name (str): The name of the sheet to retrieve values from.

    Returns:
        pd.DataFrame: DataFrame containing the sheet's data, with headers as columns.
    """
    CHUNK_ROWS: int = 5000
    MAX_EMPTY_CHUNKS: int = 2

    svc = get_sheet_service(service_account_data)

    hdr_result = (
        svc.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!1:1",
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="SERIAL_NUMBER",
        )
        .execute()
    )
    if hdr_result is None:
        raise ValueError("API returned None for header")

    headers = hdr_result.get("values", [[]])
    headers = headers[0] if headers else []

    if not any((h.strip() if isinstance(h, str) else str(h)) for h in headers):
        return pd.DataFrame()

    norm_headers = []
    seen = {}
    for i, h in enumerate(headers, start=1):
        name = str(h).strip() if h not in (None, "") else f"Column_{i}"
        base = name if name else f"Column_{i}"
        idx = seen.get(base, 0)
        norm_headers.append(base if idx == 0 else f"{base}_{idx}")
        seen[base] = idx + 1

    n_cols = len(norm_headers)
    last_col_a1 = _col_to_a1(n_cols)

    max_rows, max_cols = get_grid_limits(
        service_account_data, spreadsheet_id, sheet_name
    )

    if n_cols > max_cols:
        n_cols = max_cols
        last_col_a1 = _col_to_a1(n_cols)
        norm_headers = norm_headers[:n_cols]

    all_rows = []
    start = 2
    empty_chunks = 0

    while start <= max_rows:
        end = min(start + CHUNK_ROWS - 1, max_rows)
        a1_range = f"{sheet_name}!A{start}:{last_col_a1}{end}"

        result = (
            svc.spreadsheets()
            .values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=a1_range,
                valueRenderOption="UNFORMATTED_VALUE",
                dateTimeRenderOption="SERIAL_NUMBER",
                majorDimension="ROWS",
            )
            .execute()
        )
        if result is None:
            raise ValueError("API returned None for chunk")

        rows = result.get("values", [])
        if not rows:
            empty_chunks += 1
            if empty_chunks >= MAX_EMPTY_CHUNKS:
                break
            start = end + 1
            continue

        empty_chunks = 0

        for r in rows:
            if len(r) < n_cols:
                r = r + [""] * (n_cols - len(r))
            elif len(r) > n_cols:
                r = r[:n_cols]
            all_rows.append(r)

        start = end + 1

        if len(all_rows) >= 1_000_000:
            break

    if not all_rows:
        return pd.DataFrame(columns=norm_headers)  # type: ignore

    df = pd.DataFrame(all_rows, columns=norm_headers)  # type: ignore

    for c in df.select_dtypes(include=["object"]).columns:
        df[c] = df[c].astype(str).str.strip()

    df.replace("", pd.NA, inplace=True)

    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="ignore", downcast="integer")
        if pd.api.types.is_object_dtype(df[c]):
            df[c] = pd.to_numeric(df[c], errors="ignore", downcast="float")

    return df


def get_sheet_version(service_account_data: dict, spreadsheet_id: str) -> str:
    """
    Get the latest revision ID (version) of a Google Spreadsheet from Google Drive.

    Args:
        service_account_data (dict): Service account credentials as a dictionary.
        spreadsheet_id (str): The ID of the Google Spreadsheet.

    Returns:
        str: The latest revision ID of the spreadsheet.

    Raises:
        ValueError: If no revisions are found for the sheet.
    """
    scopes = ["https://www.googleapis.com/auth/drive.readonly"]
    creds = service_account.Credentials.from_service_account_info(
        service_account_data, scopes=scopes
    )
    service = build("drive", "v3", credentials=creds)
    result = service.revisions().list(
        fileId=spreadsheet_id, pageSize=1, fields="revisions(id)"
    ).execute()
    if result is None or not result.get("revisions"):
        raise ValueError("No revisions found for the sheet")
    return result["revisions"][0]["id"]
