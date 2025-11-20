import datetime
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

import set_mlflow  # noqa: F401
from constants import (
    LANDING_TABLE_NAME,
    PROMPT_PROFILE_TABLE_NAME,
    SILVER_RESULTS_SCHEMA,
    SILVER_STATUS_SCHEMA,
    SILVER_TASKS_RESULTS_TABLE,
    SILVER_TASKS_STATUS_TABLE,
    SILVER_TASKS_TODO_TABLE,
    MediaType,
    TaskKey,
)
from databricks.sdk.runtime import spark
from delta import DeltaTable
from models import TASK_OUTPUT_MODEL_MAP
from prompt_profiles_config import PromptTableType
from pydantic import BaseModel
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col, concat, current_timestamp, floor, lit, row_number, xxhash64
from pyspark.sql.window import Window


class ProcessingResult(BaseModel):
    status: str
    result_json: str | None
    result_binary: bytes | None = None
    error_message: str | None


class ProcessorFn(Protocol):
    def __call__(self, items: list[dict]) -> list[ProcessingResult]: ...


@dataclass
class TaskSpec:
    task_key: TaskKey
    media_type: MediaType
    prompt_table_type: PromptTableType | None
    processor: ProcessorFn
    max_attempts: int = 3

    # Optional, override default candidate generation
    build_candidates_df: Callable[[int | None], DataFrame] | None = None
    # Strategy must assign batch_id to rows and return the DataFrame plus the list of batch_id strings
    batch_strategy: Callable[[DataFrame], tuple[DataFrame, list[str]]] | None = None


def _collect_dependency_ids_from_items(items: list[dict]) -> tuple[list[list[int]], set[int]]:
    """Extract per-item dependency_result_ids normalized to ints and the deduplicated set."""
    per_item_ids: list[list[int]] = []
    all_ids: set[int] = set()
    for item in items:
        dep_ids = item.get("dependency_result_ids") or []
        dep_ids_int = [int(x) for x in dep_ids]
        per_item_ids.append(dep_ids_int)
        for rid in dep_ids_int:
            all_ids.add(int(rid))
    return per_item_ids, all_ids


def _prefetch_upstream_results(all_dependency_ids: set[int]) -> dict[int, dict]:
    """Fetch minimal upstream rows for the provided result_ids and return a mapping."""
    if not all_dependency_ids:
        return {}
    upstream_df = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(col("result_id").isin(list(all_dependency_ids)))
        .select("result_id", "task_key", "media_type", "result_json")
    )
    id_to_row: dict[int, dict] = {}
    for r in upstream_df.collect():
        rid = int(r.result_id)
        id_to_row[rid] = {
            "result_id": rid,
            "task_key": r.task_key,
            "media_type": r.media_type,
            "result_json": r.result_json,
        }
    return id_to_row


def _default_candidates_df(spec: TaskSpec, limit: int | None = None) -> DataFrame:
    if spec.prompt_table_type is None:
        raise ValueError("prompt_table_type is required for default candidate generation")

    bronze_df = spark.table(LANDING_TABLE_NAME).select(
        "file_hash", "mimetype", "file_size", "landing_id", "source_file"
    )
    bronze_df = bronze_df.filter(f"split(mimetype, '/')[0] IN ('{spec.media_type.value}')")
    if limit is not None:
        bronze_df = bronze_df.orderBy("landing_id").limit(int(limit))

    prompts_df = (
        spark.table(PROMPT_PROFILE_TABLE_NAME)
        .where(col("type") == spec.prompt_table_type.value)
        .select(
            col("id").alias("prompt_id"),
            col("version").alias("prompt_version"),
            col("prompt_hash"),
            col("content_json").alias("prompt_json"),
        )
    )

    # Pairs not present in RESULTS (silver)
    candidates_df = bronze_df.crossJoin(prompts_df)
    results_anti_df = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where((col("task_key") == lit(spec.task_key.value)) & (col("media_type") == lit(spec.media_type.value)))
        .select("file_hash", "prompt_id")
    )
    candidates_df = candidates_df.join(results_anti_df, ["file_hash", "prompt_id"], "left_anti")

    # Exclude already in TODO
    existing_todo_df = (
        spark.table(SILVER_TASKS_TODO_TABLE)
        .where((col("task_key") == lit(spec.task_key.value)) & (col("media_type") == lit(spec.media_type.value)))
        .select("file_hash", "prompt_id")
    )
    to_insert_df = candidates_df.join(existing_todo_df, ["file_hash", "prompt_id"], "left_anti")

    return to_insert_df.drop("file_size", "mimetype", "prompt_hash", "prompt_version")


def _default_batch_strategy(candidates_df: DataFrame, batch_size: int = 16) -> tuple[DataFrame, list[str]]:
    """Assign batch_id using deterministic ordering and a run_key based on current UTC time.

    Returns the DataFrame with a new batch_id column and the list of distinct batch_ids.
    FIXME: databricks only allow maximum of 1000 items.
    """
    run_key = str(int(datetime.datetime.now(datetime.UTC).timestamp()))

    w = Window.orderBy("landing_id", "file_hash", "prompt_id")
    df_with_index = candidates_df.withColumn("_rn", row_number().over(w))
    df_with_batch_index = df_with_index.withColumn("_batch_index", floor((col("_rn") - lit(1)) / lit(batch_size)))
    df_with_batch_id = df_with_batch_index.withColumn(
        "batch_id", concat(lit(run_key), lit("-"), col("_batch_index").cast("string"))
    ).drop("_rn", "_batch_index")
    # FIXME: databricks only allow maximum of 1000 items.
    df_with_batch_id = df_with_batch_id.limit(1000)

    batch_ids = [r.batch_id for r in df_with_batch_id.select("batch_id").distinct().collect()]
    return df_with_batch_id, batch_ids


def all_batch_strategy(candidates_df: DataFrame) -> tuple[DataFrame, list[str]]:
    return _default_batch_strategy(candidates_df, batch_size=1)


def build_and_enqueue_todos(spec: TaskSpec, limit: int | None = None) -> list[str]:
    """
    Build TODO items for a task and append to SILVER_TASKS_TODO_TABLE.
    Returns list of batch_id strings planned by the batch strategy.
    """
    base_df = spec.build_candidates_df(limit) if spec.build_candidates_df else _default_candidates_df(spec, limit)

    to_insert_df = (
        base_df.withColumn("task_key", lit(spec.task_key.value))
        .withColumn("media_type", lit(spec.media_type.value))
        .withColumn("queue_id", xxhash64(col("task_key"), col("media_type"), col("file_hash"), col("prompt_id")))
        .withColumn("status", lit("pending"))
        .withColumn("attempts", lit(0))
        .withColumn("queued_at", current_timestamp())
    )

    unified_cols = [
        "queue_id",
        "task_key",
        "media_type",
        "file_hash",
        "source_file",
        "prompt_id",
        "status",
        "attempts",
        "queued_at",
        "landing_id",
    ]

    # dependency_result_ids is optional; if missing, add null array
    if "dependency_result_ids" not in to_insert_df.columns:
        to_insert_df = to_insert_df.select(*unified_cols).withColumn(
            "dependency_result_ids", lit(None).cast("array<long>")
        )
    else:
        to_insert_df = to_insert_df.select(*unified_cols, "dependency_result_ids")

    # Assign batch_id using strategy
    if spec.batch_strategy is not None:
        to_insert_df, batch_ids = spec.batch_strategy(to_insert_df)
    else:
        to_insert_df, batch_ids = _default_batch_strategy(to_insert_df)

    unified_cols = [
        "queue_id",
        "batch_id",
        "task_key",
        "media_type",
        "file_hash",
        "source_file",
        "prompt_id",
        "status",
        "attempts",
        "queued_at",
        "landing_id",
    ]

    # dependency_result_ids is optional; if missing, add null array
    if "dependency_result_ids" not in to_insert_df.columns:
        to_insert_df = to_insert_df.select(*unified_cols).withColumn(
            "dependency_result_ids", lit(None).cast("array<long>")
        )
    else:
        to_insert_df = to_insert_df.select(*unified_cols, "dependency_result_ids")

    to_insert_df.write.mode("append").saveAsTable(SILVER_TASKS_TODO_TABLE)
    return batch_ids


def process_items(queue_ids: list[int], spec: TaskSpec) -> None:
    if not queue_ids:
        raise ValueError("queue_ids is required")

    # Normalize and deduplicate
    queue_ids_int = [int(q) for q in queue_ids]
    queue_ids_int = list(set(queue_ids_int))

    # Load candidate items for this spec
    items_df = spark.table(SILVER_TASKS_TODO_TABLE).where(
        (col("queue_id").isin(queue_ids_int))
        & (col("task_key") == spec.task_key.value)
        & (col("media_type") == spec.media_type.value)
    )
    if items_df.isEmpty():
        raise ValueError("No items found for provided queue_ids and spec")

    # Idempotency: remove items already present in results
    existing_results_df = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where((col("task_key") == spec.task_key.value) & (col("media_type") == spec.media_type.value))
        .select("file_hash", "prompt_id")
        .distinct()
    )
    to_process_df = items_df.join(existing_results_df, ["file_hash", "prompt_id"], "left_anti")
    if to_process_df.isEmpty():
        print("All items already processed in results, nothing to do.")
        return

    # Append batch 'processing' status
    processing_status_df = (
        to_process_df.select(
            col("queue_id"),
            col("landing_id").alias("lineage_id"),
            (col("attempts") + lit(1)).alias("attempts"),
        )
        .withColumn("task_key", lit(spec.task_key.value))
        .withColumn("media_type", lit(spec.media_type.value))
        .withColumn("event_type", lit("attempt"))
        .withColumn("status", lit("processing"))
        .withColumn("error_message", lit(None).cast("string"))
        .withColumn("result_json", lit(None).cast("string"))
        .withColumn("processed_at", current_timestamp())
        .select(
            "queue_id",
            "task_key",
            "media_type",
            "event_type",
            "status",
            "error_message",
            "result_json",
            "attempts",
            "processed_at",
            "lineage_id",
        )
    )
    processing_status_df.write.mode("append").saveAsTable(SILVER_TASKS_STATUS_TABLE)

    # Inject prompt_json via broadcast join on prompt_id when available
    # For tasks with prompt_table_type defined, fetch content_json and alias as prompt_json
    if spec.prompt_table_type is not None:
        prompts_df = (
            broadcast(spark.table(PROMPT_PROFILE_TABLE_NAME))
            .where(col("type") == spec.prompt_table_type.value)
            .select(col("id").alias("prompt_id"), col("content_json").alias("prompt_json"))
        )
        to_process_df = to_process_df.join(prompts_df, ["prompt_id"], "left")
    else:
        # For tasks without prompt table (e.g., embedding), synthesize a deterministic prompt_json
        # based on prompt_id. Keep it small for stability.
        # If no prompt_id exists, leave as null; processors should handle defaulting.
        to_process_df = to_process_df.withColumn("prompt_json", lit(None).cast("string"))

    # Collect items to driver for pure-Python batch processing (now includes prompt_json)
    item_rows = [row.asDict() for row in to_process_df.collect()]

    # Enrich items with upstream dependency results (centralized handling)
    per_item_dep_ids, all_dep_ids = _collect_dependency_ids_from_items(item_rows)
    id_to_upstream = _prefetch_upstream_results(all_dep_ids)
    # Attach both by-id dict and ordered list for convenience
    for idx, item in enumerate(item_rows):
        dep_ids_for_item = per_item_dep_ids[idx] if idx < len(per_item_dep_ids) else []
        upstream_by_id = {}
        upstream_results = []
        upstream_parsed_by_id = {}
        upstream_parsed = []
        for rid in dep_ids_for_item:
            row_dict = id_to_upstream.get(int(rid))
            if row_dict is None:
                continue
            upstream_by_id[int(rid)] = row_dict
            upstream_results.append(row_dict)
            # Parse upstream result_json deterministically using the TaskKey→Model map when possible
            try:
                _task_key = row_dict.get("task_key")
                tk = TaskKey(_task_key) if _task_key else None
                if tk is not None and tk in TASK_OUTPUT_MODEL_MAP:
                    model_cls = TASK_OUTPUT_MODEL_MAP[tk]
                    parsed = (
                        model_cls.model_validate_json(row_dict.get("result_json"))
                        if row_dict.get("result_json")
                        else None
                    )
                    if parsed is not None:
                        upstream_parsed_by_id[int(rid)] = parsed
                        upstream_parsed.append(parsed)
            except Exception:
                # Parsing issues are tolerated at enrichment time; processors can still use raw upstream_results
                pass
        item["upstream_by_id"] = upstream_by_id
        item["upstream_results"] = upstream_results
        item["upstream_parsed_by_id"] = upstream_parsed_by_id
        item["upstream_parsed"] = upstream_parsed

    final_status_rows: list[dict] = []
    result_rows: list[dict] = []

    # Resolve writer principal once per batch
    writer_principal = spark.sql("SELECT current_user() AS u").collect()[0]["u"]

    try:
        results = spec.processor(item_rows)
        if not isinstance(results, list):
            raise ValueError("Processor must return a list of ProcessingResult")
        if len(results) != len(item_rows):
            raise ValueError(f"Processor returned {len(results)} results for batch of {len(item_rows)} items")
    except Exception as e:
        tb_str = traceback.format_exc()
        shared_error = f"{e!s}\ntraceback:\n{tb_str[:1000]}"
        results = [
            ProcessingResult(status="failed", result_json=None, result_binary=None, error_message=shared_error)
            for _ in item_rows
        ]

    for item, result in zip(item_rows, results, strict=True):
        status = result.status
        result_json = result.result_json
        error_message = result.error_message
        if status not in ("processed", "failed"):
            status = "failed"
            error_message = error_message or f"Processor returned invalid status: {result.status}"

        attempts_next = int(item.get("attempts", 0)) + 1
        if status == "failed" and attempts_next >= spec.max_attempts:
            status = "dlq"

        # Validate result_json deterministically using TaskKey→Model map when available
        if status == "processed" and result_json is not None:
            try:
                model_cls = TASK_OUTPUT_MODEL_MAP.get(spec.task_key)
                if model_cls is not None:
                    # Will raise if schema mismatch
                    model_cls.model_validate_json(result_json)
            except Exception as e:
                status = "failed"
                error_message = f"output_schema_validation_failed: {e!s}"[:1000]

        final_status_rows.append(
            {
                "queue_id": int(item["queue_id"]),
                "task_key": spec.task_key.value,
                "media_type": spec.media_type.value,
                "event_type": "attempt",
                "status": status,
                "error_message": error_message,
                "result_json": result_json,
                "attempts": attempts_next,
                "processed_at": datetime.datetime.now(),
                "lineage_id": item["landing_id"],
            }
        )

        if status == "processed":
            result_rows.append(
                {
                    "result_id": int(item["queue_id"]),
                    "task_key": spec.task_key.value,
                    "media_type": spec.media_type.value,
                    "file_hash": item["file_hash"],
                    "prompt_id": item["prompt_id"],
                    "result_json": result_json,
                    "result_binary": result.result_binary if hasattr(result, "result_binary") else None,
                    "created_at": datetime.datetime.now(),
                    "created_by": writer_principal,
                    "landing_id": item["landing_id"],
                    "queue_id": int(item["queue_id"]),
                    "dependency_result_ids": item.get("dependency_result_ids"),
                }
            )

    # Append batch final statuses
    if final_status_rows:
        spark.createDataFrame(final_status_rows, schema=SILVER_STATUS_SCHEMA).write.mode("append").saveAsTable(
            SILVER_TASKS_STATUS_TABLE
        )

    # Append processed results
    if result_rows:
        spark.createDataFrame(result_rows, schema=SILVER_RESULTS_SCHEMA).write.mode("append").saveAsTable(
            SILVER_TASKS_RESULTS_TABLE
        )


def consolidate_task(spec: TaskSpec) -> None:
    # Resolve writer principal for consolidation session
    writer_principal = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
    current_state_df = spark.sql(
        f"""
    SELECT queue_id, status, error_message, attempts, processed_at, result_json, lineage_id
    FROM {SILVER_TASKS_STATUS_TABLE}
    WHERE task_key = '{spec.task_key.value}' AND media_type = '{spec.media_type.value}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY queue_id ORDER BY processed_at DESC) = 1
    """
    )

    # Update TODO with latest status/attempts
    unified_todo_delta = DeltaTable.forName(spark, SILVER_TASKS_TODO_TABLE)
    unified_todo_delta.alias("target").merge(
        current_state_df.withColumn("task_key", lit(spec.task_key.value))
        .withColumn("media_type", lit(spec.media_type.value))
        .alias("source"),
        "target.queue_id = source.queue_id AND target.task_key = source.task_key AND target.media_type = source.media_type",
    ).whenMatchedUpdate(
        set={
            "status": "source.status",
            "attempts": "source.attempts",
        }
    ).execute()

    # Consolidate successful results into unified results
    success_df = (
        current_state_df.filter("status = 'processed' AND result_json IS NOT NULL")
        .join(
            spark.table(SILVER_TASKS_TODO_TABLE)
            .where((col("task_key") == lit(spec.task_key.value)) & (col("media_type") == lit(spec.media_type.value)))
            .select("queue_id", "file_hash", "prompt_id", "dependency_result_ids"),
            "queue_id",
            "inner",
        )
        .select(
            col("file_hash"),
            col("prompt_id"),
            col("result_json"),
            lit(None).alias("result_binary"),
            col("lineage_id").alias("landing_id"),
            col("queue_id"),
            current_timestamp().alias("created_at"),
            lit(writer_principal).alias("created_by"),
            col("dependency_result_ids"),
        )
    )
    if not success_df.isEmpty():
        unified_success_df = success_df.select(
            lit(spec.task_key.value).alias("task_key"),
            lit(spec.media_type.value).alias("media_type"),
            col("queue_id").alias("result_id"),
            col("file_hash"),
            col("prompt_id"),
            col("result_json"),
            col("result_binary"),
            col("created_at"),
            col("created_by"),
            col("landing_id"),
            col("queue_id"),
            col("dependency_result_ids"),
        )
        unified_results_delta = DeltaTable.forName(spark, SILVER_TASKS_RESULTS_TABLE)
        unified_results_delta.alias("target").merge(
            unified_success_df.alias("source"),
            "target.task_key = source.task_key AND target.media_type = source.media_type AND target.file_hash = source.file_hash AND target.prompt_id = source.prompt_id",
        ).whenNotMatchedInsert(
            values={
                "task_key": "source.task_key",
                "media_type": "source.media_type",
                "result_id": "source.result_id",
                "file_hash": "source.file_hash",
                "prompt_id": "source.prompt_id",
                "result_json": "source.result_json",
                "result_binary": "source.result_binary",
                "created_at": "source.created_at",
                "created_by": "source.created_by",
                "landing_id": "source.landing_id",
                "queue_id": "source.queue_id",
                "dependency_result_ids": "source.dependency_result_ids",
            }
        ).execute()
        print(f"Consolidated {success_df.count()} successful results to unified results.")

    print("Consolidation complete.")


def process_batch(batch_id: str, spec: TaskSpec) -> None:
    if not batch_id:
        raise ValueError("batch_id is required")

    items_df = spark.table(SILVER_TASKS_TODO_TABLE).where(
        (col("batch_id") == lit(batch_id))
        & (col("task_key") == lit(spec.task_key.value))
        & (col("media_type") == lit(spec.media_type.value))
    )
    if items_df.isEmpty():
        raise ValueError(f"No items found for batch_id={batch_id} and spec")

    queue_ids = [int(r.queue_id) for r in items_df.select("queue_id").collect()]
    process_items(queue_ids, spec)
