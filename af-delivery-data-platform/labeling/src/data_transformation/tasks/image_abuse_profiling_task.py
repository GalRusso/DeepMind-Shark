import datetime
import traceback

from constants import (
    LANDING_TABLE_NAME,
    PROMPT_PROFILE_TABLE_NAME,
    SILVER_TASKS_RESULTS_TABLE,
    SILVER_TASKS_TODO_TABLE,
    MediaType,
    TaskKey,
)
from databricks.sdk.runtime import spark
from llm_orchestrator.image_abuse_profiler import ImageAbuseProfiler
from llm_orchestrator.llm_helpers import init_chat_model
from prompt_profiles_config import ImageAbuseCategory, PromptTableType
from pyspark.sql import DataFrame
from pyspark.sql.functions import array, broadcast, col, concat, floor, from_json, lit, row_number, xxhash64
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window
from task_framework import ProcessingResult, TaskSpec


def image_abuse_processor(items: list[dict]) -> list[ProcessingResult]:
    if len(items) == 0:
        return []

    assert len({i["prompt_json"] for i in items}) == 1, "All prompt_jsons must be the same to be batch processed"
    prompt_json = items[0]["prompt_json"]
    abuse_category = ImageAbuseCategory.model_validate_json(prompt_json)

    llms = []
    for model in abuse_category.models:
        llms.append(init_chat_model(model))

    # Construct the batch input using framework-enriched upstream_parsed
    file_paths = []
    descriptions = []
    taggs = []
    for item in items:
        source_file = item.get("source_file")
        description = None
        tags = None
        for upstream in item.get("upstream_parsed", []) or []:
            if hasattr(upstream, "description") or hasattr(upstream, "tags"):
                description = getattr(upstream, "description", description)
                tags = getattr(upstream, "tags", tags)
                if description is not None or (tags and len(tags) > 0):
                    break
        file_paths.append(source_file)
        descriptions.append(description)
        taggs.append(tags)

    # Actually run the profiler, in batch

    profiler = ImageAbuseProfiler(
        abuse_category=abuse_category,
        llms=llms,
    )
    results: list[ProcessingResult] = []

    try:
        batch_results = profiler.batch(file_paths, descriptions, taggs)
        for r in batch_results:
            results.append(ProcessingResult(status="processed", result_json=r.model_dump_json(), error_message=None))
    except Exception as e:
        tb_str = traceback.format_exc()
        shared_error = f"{e!s}\ntraceback:\n{tb_str[:1000]}"
        for _ in range(len(file_paths)):
            results.append(ProcessingResult(status="failed", result_json=None, error_message=shared_error))

    return results


def build_abuse_candidates_df(limit: int | None = None):
    bronze_df = spark.table(LANDING_TABLE_NAME).select(
        "file_hash", "mimetype", "file_size", "landing_id", "source_file"
    )
    bronze_df = bronze_df.filter("split(mimetype, '/')[0] IN ('image')")
    if limit is not None:
        bronze_df = bronze_df.orderBy("landing_id")
        bronze_df = bronze_df.limit(limit)

    schema = StructType([StructField("pt_image_desc_tag_hash", StringType())])
    prompts_df = (
        spark.table(PROMPT_PROFILE_TABLE_NAME)
        .where(col("type") == PromptTableType.image_abuse_category.value)
        .withColumn("json_struct", from_json("content_json", schema))
        .withColumn("pt_image_desc_tag_hash", col("json_struct.pt_image_desc_tag_hash"))
        .select(
            col("id").alias("prompt_id"),
            col("version").alias("prompt_version"),
            col("prompt_hash"),
            col("content_json").alias("prompt_json"),
            col("pt_image_desc_tag_hash"),
        )
    )

    desc_tag_prompt_df = (
        spark.table(PROMPT_PROFILE_TABLE_NAME)
        .where(col("type") == PromptTableType.image_description_tag.value)
        .select(col("id").alias("prompt_des_tag_id"), col("prompt_hash").alias("pt_image_desc_tag_hash"))
    )

    prompts_df = prompts_df.join(desc_tag_prompt_df, "pt_image_desc_tag_hash", "inner")

    silver_df = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where((col("task_key") == lit(TaskKey.image_abuse.value)) & (col("media_type") == lit(MediaType.image.value)))
        .select("file_hash", "prompt_id")
    )

    existing_todo_df = (
        spark.table(SILVER_TASKS_TODO_TABLE)
        .where((col("task_key") == lit(TaskKey.image_abuse.value)) & (col("media_type") == lit(MediaType.image.value)))
        .select("file_hash", "prompt_id")
    )

    candidates_df = bronze_df.join(broadcast(prompts_df), how="cross")
    candidates_df = candidates_df.join(silver_df, ["file_hash", "prompt_id"], "left_anti")
    to_insert_df = candidates_df.join(existing_todo_df, ["file_hash", "prompt_id"], "left_anti")

    des_tag_success_df = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.image_des_tag.value)) & (col("media_type") == lit(MediaType.image.value))
        )
        .select("file_hash", col("prompt_id").alias("prompt_des_tag_id"))
    )
    to_insert_df = to_insert_df.join(des_tag_success_df, ["file_hash", "prompt_des_tag_id"], "inner")
    to_insert_df = to_insert_df.withColumn(
        "upstream_result_id",
        xxhash64(
            lit(TaskKey.image_des_tag.value), lit(MediaType.image.value), col("file_hash"), col("prompt_des_tag_id")
        ),
    )

    # Shape to the columns the framework expects; dependency_result_ids is optional but we include it

    to_insert_df = to_insert_df.drop(
        "file_size", "mimetype", "prompt_hash", "prompt_version", "pt_image_desc_tag_hash"
    ).withColumn("dependency_result_ids", array(col("upstream_result_id")))

    return to_insert_df


def build_batch_strategy(candidates_df: DataFrame) -> tuple[DataFrame, list[str]]:
    """
    Batch by prompt_id to ensure each batch contains homogeneous prompts.
    Assigns a run-scoped batch_id per chunk using a UTC timestamp run_key.
    Returns (DataFrame with batch_id, list of distinct batch_ids).
    """
    DEFAULT_BATCH_SIZE = 16

    run_key = str(int(datetime.datetime.now(datetime.UTC).timestamp()))

    w = Window.partitionBy("prompt_id").orderBy("landing_id", "file_hash", "prompt_id")
    df_with_index = candidates_df.withColumn("_rn", row_number().over(w))
    df_with_batch_index = df_with_index.withColumn(
        "_batch_index", floor((col("_rn") - lit(1)) / lit(DEFAULT_BATCH_SIZE))
    )
    df_with_batch_id = df_with_batch_index.withColumn(
        "batch_id",
        concat(
            lit(run_key),
            lit("-"),
            col("prompt_id").substr(1, 8),
            lit("-"),
            col("_batch_index").cast("string"),
        ),
    ).drop("_rn", "_batch_index")

    batch_ids = [r.batch_id for r in df_with_batch_id.select("batch_id").distinct().collect()]
    return df_with_batch_id, batch_ids


def get_image_abuse_spec() -> TaskSpec:
    return TaskSpec(
        task_key=TaskKey.image_abuse,
        media_type=MediaType.image,
        prompt_table_type=PromptTableType.image_abuse_category,
        processor=image_abuse_processor,
        max_attempts=3,
        build_candidates_df=build_abuse_candidates_df,
        batch_strategy=build_batch_strategy,
    )


if __name__ == "__main__":
    # Minimal happy-path: plan -> process -> consolidate
    from databricks.sdk.runtime import spark
    from task_framework import build_and_enqueue_todos, consolidate_task, process_batch

    spark.sql("DELETE FROM af_delivery_dev.silver_labeling.tasks_status where task_key = 'image.abuse';")
    spark.sql("DELETE FROM af_delivery_dev.silver_labeling.tasks_todo where task_key = 'image.abuse';")
    spark.sql("DELETE FROM af_delivery_dev.silver_labeling.tasks_results where task_key = 'image.abuse';")

    spec = get_image_abuse_spec()
    print("[image_abuse_task] Planning a small batch (limit=1)...")
    batch_ids = build_and_enqueue_todos(spec, limit=1)
    print(f"[image_abuse_task] Planned {len(batch_ids)} batch(es): {batch_ids}")
    # expect 5 batches because of 5 abuse categories
    assert len(batch_ids) == 5, f"Expected 5 batches, got {len(batch_ids)}"
    for bid in batch_ids:
        print(f"[image_abuse_task] Processing batch_id={bid}...")
        process_batch(bid, spec)
    print("[image_abuse_task] Consolidating...")
    consolidate_task(spec)
    # Optional: small visibility into results count
    results_cnt = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where((col("task_key") == lit(TaskKey.image_abuse.value)) & (col("media_type") == lit(MediaType.image.value)))
        .count()
    )
    print(f"[image_abuse_task] Done. Current total results for task={TaskKey.image_abuse.value}: {results_cnt}")
