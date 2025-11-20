from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional
import json
import time

import requests
import re

try:
    import httpx
except ImportError:
    httpx = None  # Optional dependency for async functions


def clean_column_name(name: str) -> str:
    """
    Transform a Monday column title into a normalized string containing only
    lowercase letters, numbers, and underscores.

    - Lowercases the name
    - Replaces any sequence of non-word characters with a single underscore
    - Collapses multiple underscores into one
    - Strips leading/trailing underscores

    Parameters
    ----------
    name: str
        Original Monday column title.

    Returns
    -------
    str
        Normalized column name (only a-z, 0-9, and underscores).
    """
    name = name.lower()
    # Replace any sequence of non-word characters with underscore
    name = re.sub(r"\W+", "_", name)
    # Collapse multiple underscores into one
    name = re.sub(r"_+", "_", name)
    # Strip leading/trailing underscores
    name = name.strip("_")
    return name


def get_board_items(
    api_key: str,
    board_id: str,
    group_id: Optional[str] = None,
    *,
    get_subitems: bool = True,
    get_mirror_columns: bool = True,
    to_clean_columns: bool = True,
    include_columns: Optional[List[str]] = None,
    exclude_columns: Optional[List[str]] = None,
    limit: int = 500,
    max_pages: int = 1000,
    sleep_seconds: float = 0.5,
    session: Optional[requests.Session] = None,
    timeout_seconds: float = 60.0,
    filter_dates: Optional[list[str]] = None,
    date_column_id: Optional[str] = "__last_updated__",
    order_by_column_id: Optional[str] = "__last_updated__",
    order_by_direction: Optional[Literal["asc", "desc"]] = "desc",
) -> List[Dict[str, Any]]:
    """
    Fetch items and subitems (optional) from a Monday board via GraphQL.

    This function mirrors the logic used in the notebook while making it reusable
    and configurable. It supports optional scoping to a specific group and paginates
    through results using the items_page cursor.

    Parameters
    ----------
    api_key: str
        Monday API key for authentication. Pass in securely from your secrets store.
    board_id: str
        ID of the Monday board to read from.
    group_id: Optional[str]
        If provided, limits results to a specific group within the board.
    get_subitems: bool, default True
        If True, fetch subitems for each item. If False, subitems will be empty lists.
    get_mirror_columns: bool, default True
        If True, fetch display_value for mirror columns. If False, mirror columns will return null.
        Note: Mirror columns always return null for the 'text' field, so display_value is required
        to get the actual content of mirrored columns.
    to_clean_columns: bool, default True
        If True, normalize column titles using `clean_column_name`. Otherwise, use raw titles.
    include_columns: Optional[List[str]], default None
        If provided, only include these columns in the results. Column names should match
        the cleaned column names if to_clean_columns=True, or raw column titles if False.
        Mutually exclusive with exclude_columns.
    exclude_columns: Optional[List[str]], default None
        If provided, exclude these columns from the results. Column names should match
        the cleaned column names if to_clean_columns=True, or raw column titles if False.
        Mutually exclusive with include_columns.
    limit: int, default 500
        Limit the number of items to fetch per page
    max_pages: int, default 1000
        Safety cap on number of pages to fetch.
    sleep_seconds: float, default 0.5
        Delay between page fetches to be polite to the API.
    session: Optional[requests.Session]
        Optional session to reuse connections/config; created if not provided.
    timeout_seconds: float, default 30.0
        HTTP request timeout.
    filter_dates: Optional[list[str]], default None
        List of date filters. Can contain special values "YESTERDAY", "TODAY", or actual dates in YYYY-MM-DD format.
        If None, no date filtering is applied. Example: ["YESTERDAY", "TODAY"] or ["2024-01-15", "2024-01-16"]
    date_column_id: Optional[str], default None
        The ID of the date column to filter by. Required when filter_dates is provided.
    order_by_column_id: Optional[str], default "__last_updated__"
        The ID of the column to order by. Required when order_by_column_id is provided.
    order_by_direction: Optional[str], default "desc"
        The direction to order by. Required when order_by_column_id is provided.

    Returns
    -------
    List[Dict[str, Any]]
        List of items where each item includes flattened `column_values` and a
        `subitems` list with the same treatment. If get_subitems is False, subitems
        will be empty lists.
    """

    api_url = "https://api.monday.com/v2"
    http = session or requests.Session()
    headers = {
        "Content-Type": "application/json",
        "Authorization": api_key,
    }

    # Step 1: Fetch Column Metadata
    columns = get_columns(api_key, board_id, timeout_seconds=timeout_seconds, session=session)

    if to_clean_columns:
        columns = {
            col["id"]: clean_column_name(col["title"])
            for col in columns
        }
    else:
        columns = {
            col["id"]: col["title"]
            for col in columns
        }

    # Validate mutually exclusive parameters
    if include_columns is not None and exclude_columns is not None:
        raise ValueError("include_columns and exclude_columns are mutually exclusive. Use only one or neither.")

    # Filter columns based on include_columns or exclude_columns
    if include_columns:
        include_set = set(include_columns)
        # Keep only columns that are in the include_columns list
        columns = {
            col_id: col_name
            for col_id, col_name in columns.items()
            if col_name in include_set
        }
    elif exclude_columns:
        exclude_set = set(exclude_columns)
        # Remove columns that are in the exclude_columns list
        columns = {
            col_id: col_name
            for col_id, col_name in columns.items()
            if col_name not in exclude_set
        }

    # Step 2: Build timestamp filtering rules based on filter_dates
    timestamp_rules = None
    
    if filter_dates:
        # Build compare_value array based on Monday.com API requirements
        compare_values = []
        for date in filter_dates:
            if date in ["YESTERDAY", "TODAY", "TOMORROW", "THIS_WEEK", "LAST_WEEK", "THIS_MONTH", "LAST_MONTH", "PAST_DATETIME", "FUTURE_DATETIME", "UPCOMING", "OVERDUE"]:
                # Special values can be used directly
                compare_values.append(date)
            else:
                # For actual dates, use EXACT format as required by Monday.com API
                if not (len(date) == 10 and date[4] == '-' and date[7] == '-'):
                    raise ValueError(f"Invalid date format: {date}. Expected YYYY-MM-DD format or special values like 'YESTERDAY'/'TODAY'")
                compare_values.extend(["EXACT", date])
        
        # Build timestamp rules according to Monday.com API format
        if date_column_id in ["__last_updated__"]:
            # For last updated column, use UPDATED_AT attribute
            timestamp_rules = f'[{{column_id: "{date_column_id}", compare_value: {json.dumps(compare_values)}, operator: any_of, compare_attribute: "UPDATED_AT"}}]'
        else:
            # For regular date columns, no compare_attribute needed
            timestamp_rules = f'[{{column_id: "{date_column_id}", compare_value: {json.dumps(compare_values)}, operator: any_of}}]'

    # Step 3: Iterate over paginated items
    all_items: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    previous_cursor: Optional[str] = None
    page_count = 1
    
    # Build fragments conditionally based on parameters
    subitems_field = """
                        subitems {
                        id
                        name
                        column_values {
                            id
                            text
                            value
                        }
                        }""" if get_subitems else ""
    
    
    mirror_fragment = """
                            ... on MirrorValue {
                            id
                            display_value
                            }""" if get_mirror_columns else ""
    
    board_relation_fragment = """
                            ... on BoardRelationValue {
                            id
                            linked_item_ids
                            linked_items {
                                id
                                name
                            }
                            }""" if get_mirror_columns else ""
    
    # Build query_params for filtering
    timestamp_rules_fragment = f', rules: {timestamp_rules}' if timestamp_rules else ""
    # Build query_params for ordering by last updated column
    query_params = f', query_params: {{order_by: [{{column_id: "{order_by_column_id}", direction: {order_by_direction}}}] {timestamp_rules_fragment}}}'

    # Getting items from Monday
    while page_count <= max_pages:
        print(f"Processing page {page_count=}")
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

        cursor_fragment = f", cursor:\"{cursor}\"" if cursor else ""
        query_params = query_params if not cursor else ""
        
        if group_id:
            item_query = """
            {{
                boards(ids: {board_id}) {{
                    groups(ids:["{group_id}"]) {{
                        items_page(limit:{limit}{cursor_fragment}{query_params}) {{
                        cursor
                        items {{
                            id
                            name
                            column_values {{
                                id
                                text
                                value
                                {mirror_fragment}
                                {board_relation_fragment}
                            }}
                            {subitems_field}
                        }}
                    }}
                }}
            }}
        }}""".format(board_id=board_id, group_id=group_id, limit=limit, cursor_fragment=cursor_fragment, query_params=query_params, subitems_field=subitems_field, mirror_fragment=mirror_fragment, board_relation_fragment=board_relation_fragment)
        else:
            item_query = """
            {{
              boards (ids: {board_id}) {{
                items_page (limit:{limit}{cursor_fragment}{query_params}) {{
                  cursor
                  items {{
                    id
                    name
                    column_values {{
                      id
                      text
                      value
                      {mirror_fragment}
                      {board_relation_fragment}
                    }}
                    {subitems_field}
                  }}
                }}
              }}
            }}""".format(board_id=board_id, limit=limit, cursor_fragment=cursor_fragment, query_params=query_params, subitems_field=subitems_field, mirror_fragment=mirror_fragment, board_relation_fragment=board_relation_fragment)

        item_response = http.post(
            api_url,
            json={"query": item_query},
            headers=headers,
            timeout=timeout_seconds,
        )
        if item_response.status_code != 200:
            raise RuntimeError(
                f"API request for items failed with status code {item_response.status_code}: "
                f"{item_response.text}"
            )

        item_data = item_response.json()
        if "errors" in item_data:
            # Surface GraphQL errors to caller
            raise RuntimeError(f"GraphQL errors: {json.dumps(item_data['errors'])}")

        try:
            if group_id:
                items_page = item_data["data"]["boards"][0]["groups"][0]["items_page"]
            else:
                items_page = item_data["data"]["boards"][0]["items_page"]
        except Exception as exc:  # return diagnostic info to aid troubleshooting
            raise RuntimeError(
                "Error extracting items_page from response. "
                f"Query: {item_query} Response: {json.dumps(item_data)[:2000]}"
            ) from exc

        cursor = items_page.get("cursor")
        items = items_page.get("items", [])

        # Step 4: Map Column Titles to Values, including subitems
        for item in items:
            subitems_list: List[Dict[str, Any]] = []
            if get_subitems and item.get("subitems"):
                for sub in item["subitems"]:
                    subitem_data = {
                        "id": sub.get("id"),
                        "name": sub.get("name"),
                        **{
                            columns.get(col_val.get("id", ""), col_val.get("id")): (
                                col_val.get("display_value") or col_val.get("text") or col_val.get("value")
                            )
                            for col_val in sub.get("column_values", [])
                        },
                    }
                    subitems_list.append(subitem_data)

            item_record: Dict[str, Any] = {
                "name": item.get("name"),
                "id": item.get("id"),
                **{
                    columns[col_val["id"]]: (
                        # Handle board relation columns - extract linked item names
                        [linked_item.get("name") for linked_item in col_val.get("linked_items", [])] if col_val.get("linked_items") else (
                            col_val.get("display_value") or col_val.get("text") or col_val.get("value")
                        )
                    )
                    for col_val in item.get("column_values", [])
                    if col_val.get("id") in columns
                },
                "subitems": subitems_list,
            }
            
            # Remove subitems if not needed
            if not get_subitems:
                del item_record["subitems"]
                
            
            to_delete_cols = []
            if include_columns:
                # Ensure to remove existing columns that are NOT in include_columns
                to_delete_cols = [col for col in item_record.keys() if col not in include_columns]
            elif exclude_columns:
                # Ensure to remove existing columns that are in exclude_columns
                to_delete_cols = [col for col in item_record.keys() if col in exclude_columns]
            
            for col in to_delete_cols:
                del item_record[col]
            
            all_items.append(item_record)

        if cursor is None or cursor == previous_cursor:
            break

        previous_cursor = cursor
        page_count += 1

    return all_items


def get_groups_by_board_id(
    api_key: str,
    board_id: str,
    timeout_seconds: float = 30.0,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch groups from a Monday board via GraphQL.
    """
    api_url = "https://api.monday.com/v2"
    http = session or requests.Session()
    headers = {
        "Content-Type": "application/json",
        "Authorization": api_key,
    }
    # The "name" field is not available on the Group type in the Monday API.
    # Only request fields that are supported, such as "id" and "title".
    query = f"""
    {{
      boards(ids: {board_id}) {{
        groups {{
          id
          title
        }}
      }}
    }}"""

    response = http.post(
        api_url,
        json={"query": query},
        headers=headers,
        timeout=timeout_seconds,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"API request for groups failed with status code {response.status_code}: "
            f"{response.text}"
        )

    resp_json = response.json()
    if "errors" in resp_json:
        raise RuntimeError(f"GraphQL errors: {json.dumps(resp_json['errors'])}")

    # Return the groups with "id" and "title" fields
    return resp_json["data"]["boards"][0]["groups"]


def get_columns(
    api_key: str,
    board_id: str,
    *,
    timeout_seconds: float = 30.0,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch columns from a Monday board via GraphQL, optionally filtered by group.

    Parameters
    ----------
    api_key: str
        Monday API key for authentication. Pass in securely from your secrets store.
    board_id: str
        ID of the Monday board to read from.
    group_id: Optional[str]
        If provided, returns columns from items in the specified group.
    to_clean_columns: bool, default True
        If True, normalize column titles using `clean_column_name`. Otherwise, use raw titles.
    timeout_seconds: float, default 30.0
        HTTP request timeout.
    session: Optional[requests.Session]
        Optional session to reuse connections/config; created if not provided.

    Returns
    -------
    List[Dict[str, Any]]
        List of columns with their properties (id, title, type, etc.).
    """
    api_url = "https://api.monday.com/v2"
    http = session or requests.Session()
    headers = {
        "Content-Type": "application/json",
        "Authorization": api_key,
    }

    query = """
    {{
        boards(ids: {board_id}) {{
            columns {{
                id
                title
            }}
        }}
    }}""".format(board_id=board_id)

    response = http.post(
        api_url,
        json={"query": query},
        headers=headers,
        timeout=timeout_seconds,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"API request for columns failed with status code {response.status_code}: "
            f"{response.text}"
        )

    resp_json = response.json()
    if "errors" in resp_json:
        raise RuntimeError(f"GraphQL errors: {json.dumps(resp_json['errors'])}")

    return resp_json["data"]["boards"][0]["columns"]


def get_column_settings(
    api_key: str,
    board_id: str,
    column_id: str,
    *,
    timeout_seconds: float = 30.0,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """
    Fetch detailed settings for a specific column, including possible values for dropdown columns.

    This function retrieves the column's metadata including its type and settings_str, which
    contains configuration like dropdown labels, status labels, and other column-specific settings.

    Parameters
    ----------
    api_key : str
        Monday API key for authentication. Pass in securely from your secrets store.
    board_id : str
        ID of the Monday board containing the column.
    column_id : str
        ID of the column to fetch settings for.
    timeout_seconds : float, default 30.0
        HTTP request timeout.
    session : Optional[requests.Session]
        Optional session to reuse connections/config; created if not provided.

    Returns
    -------
    Dict[str, Any]
        Dictionary containing column information:
        - id: Column ID
        - title: Column title/name
        - type: Column type (e.g., "dropdown", "status", "text", "date")
        - settings: Parsed settings dictionary (from settings_str JSON)
        - labels: List of possible values (for dropdown/status columns only)

    Raises
    ------
    RuntimeError
        If the API request fails or returns GraphQL errors.
    ValueError
        If the column is not found on the specified board.

    Notes
    -----
    Common column types and their settings:

    - **Dropdown columns** (type="dropdown"):
        Returns labels array with possible dropdown values
    - **Status columns** (type="status"):
        Returns labels array with status options and their colors
    - **Text columns** (type="text"):
        No special settings, accepts any string
    - **Date columns** (type="date"):
        No labels, accepts date values
    - **People columns** (type="people"):
        No labels, references user IDs

    Examples
    --------
    Get dropdown column settings:

    >>> settings = get_column_settings(
    ...     api_key="your_api_key",
    ...     board_id="9876543210",
    ...     column_id="dropdown"
    ... )
    >>> print(settings["labels"])
    ['Option 1', 'Option 2', 'Option 3']

    Get status column settings:

    >>> settings = get_column_settings(
    ...     api_key="your_api_key",
    ...     board_id="9876543210",
    ...     column_id="status"
    ... )
    >>> print(settings["labels"])
    ['Working on it', 'Done', 'Stuck']

    Check column type before using:

    >>> settings = get_column_settings(api_key, board_id, "text_col")
    >>> if settings["type"] == "dropdown":
    ...     print(f"Possible values: {settings['labels']}")
    ... else:
    ...     print(f"Column type '{settings['type']}' has no predefined values")
    """
    api_url = "https://api.monday.com/v2"
    http = session or requests.Session()
    headers = {
        "Content-Type": "application/json",
        "Authorization": api_key,
    }

    # Query to get column details including settings_str
    query = f"""
    {{
        boards(ids: {board_id}) {{
            columns {{
                id
                title
                type
                settings_str
            }}
        }}
    }}"""

    response = http.post(
        api_url,
        json={"query": query},
        headers=headers,
        timeout=timeout_seconds,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"API request for column settings failed with status code {response.status_code}: "
            f"{response.text}"
        )

    resp_json = response.json()
    if "errors" in resp_json:
        raise RuntimeError(f"GraphQL errors: {json.dumps(resp_json['errors'])}")

    # Find the specific column
    columns = resp_json["data"]["boards"][0]["columns"]
    column = next((col for col in columns if col["id"] == column_id), None)

    if column is None:
        raise ValueError(
            f"Column '{column_id}' not found in board {board_id}. "
            f"Available columns: {[col['id'] for col in columns]}"
        )

    # Parse settings_str JSON
    settings = {}
    if column.get("settings_str"):
        try:
            settings = json.loads(column["settings_str"])
        except json.JSONDecodeError:
            settings = {"raw": column["settings_str"]}

    # Build result dictionary
    result = {
        "id": column["id"],
        "title": column["title"],
        "type": column["type"],
        "settings": settings,
    }

    # Extract labels for dropdown and status columns
    if column["type"] in ["dropdown", "status"] and "labels" in settings:
        # For dropdown/status, extract just the label names
        labels_data = settings["labels"]
        if isinstance(labels_data, dict):
            # Status columns use dict format: {0: "Label1", 1: "Label2"}
            result["labels"] = list(labels_data.values())
        elif isinstance(labels_data, list):
            # Some formats use list of dicts with "name" key
            if labels_data and isinstance(labels_data[0], dict):
                result["labels"] = [label.get("name", label) for label in labels_data]
            else:
                result["labels"] = labels_data
        else:
            result["labels"] = []
    else:
        result["labels"] = None

    return result


def get_views(
    api_key: str,
    board_id: str,
    *,
    timeout_seconds: float = 30.0,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch views from a Monday board via GraphQL.

    Parameters
    ----------
    api_key: str
        Monday API key for authentication. Pass in securely from your secrets store.
    board_id: str
        ID of the Monday board to read from.
    timeout_seconds: float, default 30.0
        HTTP request timeout.
    session: Optional[requests.Session]
        Optional session to reuse connections/config; created if not provided.

    Returns
    -------
    List[Dict[str, Any]]
        List of views with their properties (id, name, type, settings_str, etc.).
    """
    api_url = "https://api.monday.com/v2"
    http = session or requests.Session()
    headers = {
        "Content-Type": "application/json",
        "Authorization": api_key,
    }

    # Views can only be queried from the board level, not from groups
    query = f"""
    {{
      boards(ids: {board_id}) {{
        views {{
          id
          name
          type
          settings_str
        }}
      }}
    }}"""

    response = http.post(
        api_url,
        json={"query": query},
        headers=headers,
        timeout=timeout_seconds,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"API request for views failed with status code {response.status_code}: "
            f"{response.text}"
        )

    resp_json = response.json()
    if "errors" in resp_json:
        raise RuntimeError(f"GraphQL errors: {json.dumps(resp_json['errors'])}")

    return resp_json["data"]["boards"][0]["views"]


def update_item(
    api_key: str,
    item_id: str,
    column_values: Dict[str, Any],
    board_id: str,
    *,
    timeout_seconds: float = 30.0,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """
    Update column values for an existing Monday.com item using GraphQL mutations.

    This function uses the change_multiple_column_values mutation to update one or more
    columns of a Monday item. Column values must be formatted according to Monday.com's
    API requirements for each column type.

    Parameters
    ----------
    api_key : str
        Monday API key for authentication. Pass in securely from your secrets store.
    item_id : str
        ID of the Monday item to update.
    column_values : Dict[str, Any]
        Dictionary mapping column IDs to their new values. Values must be formatted
        according to Monday.com's API requirements for each column type.
    board_id : str
        ID of the Monday board containing the item.
    timeout_seconds : float, default 30.0
        HTTP request timeout.
    session : Optional[requests.Session]
        Optional session to reuse connections/config; created if not provided.

    Returns
    -------
    Dict[str, Any]
        Dictionary containing the updated item's id and name.
        Structure: {"id": "<item_id>", "name": "<item_name>"}

    Raises
    ------
    RuntimeError
        If the API request fails or returns GraphQL errors.
    ValueError
        If column_values is not properly formatted for Monday.com API.

    Notes
    -----
    Column values must be formatted according to Monday.com's API requirements:

    - **Text columns**: Simple string
        ``"text value"``
    - **Status columns**: Dictionary with label
        ``{"label": "Status Label"}``
    - **Date columns**: Dictionary with date (and optional time)
        ``{"date": "YYYY-MM-DD"}`` or ``{"date": "YYYY-MM-DD", "time": "HH:MM:SS"}``
    - **Number columns**: String representation
        ``"123"`` or ``"123.45"``
    - **Person columns**: Dictionary with personsAndTeams array
        ``{"personsAndTeams": [{"id": 12345, "kind": "person"}]}``
    - **Dropdown columns**: Dictionary with labels array
        ``{"labels": ["Option1"]}``

    Examples
    --------
    Update a single status column:

    >>> update_item(
    ...     api_key="your_api_key",
    ...     item_id="1234567890",
    ...     board_id="9876543210",
    ...     column_values={"status": {"label": "Done"}}
    ... )
    {'id': '1234567890', 'name': 'Task Name'}

    Update multiple columns simultaneously:

    >>> update_item(
    ...     api_key="your_api_key",
    ...     item_id="1234567890",
    ...     board_id="9876543210",
    ...     column_values={
    ...         "status": {"label": "In Progress"},
    ...         "text": "Updated description",
    ...         "date4": {"date": "2025-11-03"}
    ...     }
    ... )
    {'id': '1234567890', 'name': 'Task Name'}

    Update with session reuse for better performance:

    >>> session = requests.Session()
    >>> update_item(
    ...     api_key="your_api_key",
    ...     item_id="1234567890",
    ...     board_id="9876543210",
    ...     column_values={"status": {"label": "Done"}},
    ...     session=session
    ... )
    {'id': '1234567890', 'name': 'Task Name'}
    """
    api_url = "https://api.monday.com/v2"
    http = session or requests.Session()
    headers = {
        "Content-Type": "application/json",
        "Authorization": api_key,
    }

    # Format column values as JSON string and escape quotes for GraphQL
    column_values_json = json.dumps(column_values).replace('"', '\\"')

    # Build GraphQL mutation
    query = f"""
    mutation {{
        change_multiple_column_values(
            item_id: {item_id},
            board_id: {board_id},
            column_values: "{column_values_json}"
        ) {{
            id
            name
        }}
    }}"""

    # Execute request
    response = http.post(
        api_url,
        json={"query": query},
        headers=headers,
        timeout=timeout_seconds,
    )

    # Check HTTP status
    if response.status_code != 200:
        raise RuntimeError(
            f"API request to update item failed with status code {response.status_code}: "
            f"{response.text}"
        )

    # Parse response and check for GraphQL errors
    resp_json = response.json()
    if "errors" in resp_json:
        raise RuntimeError(
            f"GraphQL errors when updating item {item_id}: {json.dumps(resp_json['errors'])}"
        )

    # Return updated item data
    return resp_json["data"]["change_multiple_column_values"]


async def aupdate_item(
    api_key: str,
    item_id: str,
    column_values: Dict[str, Any],
    board_id: str,
    *,
    timeout_seconds: float = 30.0,
    session: Optional["httpx.AsyncClient"] = None,
) -> Dict[str, Any]:
    """
    Async version: Update column values for an existing Monday.com item using GraphQL mutations.

    This async function uses httpx for non-blocking HTTP requests, allowing multiple updates
    to run concurrently. Perfect for batch operations or when you need high performance.

    Parameters
    ----------
    api_key : str
        Monday API key for authentication. Pass in securely from your secrets store.
    item_id : str
        ID of the Monday item to update.
    column_values : Dict[str, Any]
        Dictionary mapping column IDs to their new values. Values must be formatted
        according to Monday.com's API requirements for each column type.
    board_id : str
        ID of the Monday board containing the item.
    timeout_seconds : float, default 30.0
        HTTP request timeout.
    session : Optional[httpx.AsyncClient]
        Optional async client to reuse connections; created if not provided.

    Returns
    -------
    Dict[str, Any]
        Dictionary containing the updated item's id and name.
        Structure: {"id": "<item_id>", "name": "<item_name>"}

    Raises
    ------
    RuntimeError
        If the API request fails or returns GraphQL errors.
    ImportError
        If httpx is not installed. Install with: pip install httpx

    Notes
    -----
    Column values must be formatted according to Monday.com's API requirements:

    - **Text columns**: Simple string
        ``"text value"``
    - **Status columns**: Dictionary with label
        ``{"label": "Status Label"}``
    - **Date columns**: Dictionary with date (and optional time)
        ``{"date": "YYYY-MM-DD"}`` or ``{"date": "YYYY-MM-DD", "time": "HH:MM:SS"}``
    - **Number columns**: String representation
        ``"123"`` or ``"123.45"``
    - **Person columns**: Dictionary with personsAndTeams array
        ``{"personsAndTeams": [{"id": 12345, "kind": "person"}]}``
    - **Dropdown columns**: Dictionary with labels array
        ``{"labels": ["Option1"]}``

    Examples
    --------
    Single update with async/await:

    >>> result = await aupdate_item(
    ...     api_key="your_api_key",
    ...     item_id="1234567890",
    ...     board_id="9876543210",
    ...     column_values={"status": {"label": "Done"}}
    ... )
    {'id': '1234567890', 'name': 'Task Name'}

    Batch updates with session reuse (much faster):

    >>> import asyncio
    >>> async with httpx.AsyncClient() as session:
    ...     tasks = [
    ...         aupdate_item(api_key, item_id_1, board_id, {"status": {"label": "Done"}}, session=session),
    ...         aupdate_item(api_key, item_id_2, board_id, {"status": {"label": "Done"}}, session=session),
    ...         aupdate_item(api_key, item_id_3, board_id, {"status": {"label": "Done"}}, session=session),
    ...     ]
    ...     results = await asyncio.gather(*tasks)

    Concurrent updates (runs in parallel):

    >>> import asyncio
    >>> results = await asyncio.gather(
    ...     aupdate_item(api_key, "111", board_id, {"status": {"label": "Done"}}),
    ...     aupdate_item(api_key, "222", board_id, {"status": {"label": "Done"}}),
    ...     aupdate_item(api_key, "333", board_id, {"status": {"label": "Done"}}),
    ... )
    """
    if httpx is None:
        raise ImportError(
            "httpx is required for async functions. Install with: pip install httpx"
        )

    api_url = "https://api.monday.com/v2"
    headers = {
        "Content-Type": "application/json",
        "Authorization": api_key,
    }

    # Format column values as JSON string and escape quotes for GraphQL
    column_values_json = json.dumps(column_values).replace('"', '\\"')

    # Build GraphQL mutation
    query = f"""
    mutation {{
        change_multiple_column_values(
            item_id: {item_id},
            board_id: {board_id},
            column_values: "{column_values_json}"
        ) {{
            id
            name
        }}
    }}"""

    # Execute async request
    if session:
        # Reuse provided session
        response = await session.post(
            api_url,
            json={"query": query},
            headers=headers,
            timeout=timeout_seconds,
        )
    else:
        # Create temporary session for single request
        async with httpx.AsyncClient() as client:
            response = await client.post(
                api_url,
                json={"query": query},
                headers=headers,
                timeout=timeout_seconds,
            )

    # Check HTTP status
    if response.status_code != 200:
        raise RuntimeError(
            f"API request to update item failed with status code {response.status_code}: "
            f"{response.text}"
        )

    # Parse response and check for GraphQL errors
    resp_json = response.json()
    if "errors" in resp_json:
        raise RuntimeError(
            f"GraphQL errors when updating item {item_id}: {json.dumps(resp_json['errors'])}"
        )

    # Return updated item data
    return resp_json["data"]["change_multiple_column_values"]
