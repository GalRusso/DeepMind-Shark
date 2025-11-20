from constants import LANDING_TABLE_NAME, SILVER_TASKS_RESULTS_TABLE, SILVER_TASKS_TODO_TABLE, MediaType, TaskKey
from databricks.sdk.runtime import spark
from llm_orchestrator.video_abuse_profiler import VideoAbuseFrameResult
from llm_orchestrator.video_frame_abuse_consolidation import consolidate_abuse_categories
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, collect_list, lit
from task_framework import ProcessingResult, TaskSpec


def build_candidates_df(limit: int | None = None) -> DataFrame:
    """
    require video.frame_abuse results and enqueue a per-video consolidation task.
    """

    bronze_df = (
        spark.table(LANDING_TABLE_NAME)
        .select("file_hash", "mimetype", "file_size", "landing_id", "source_file")
        .filter("split(mimetype, '/')[0] IN ('video')")
    )

    # Single prompt_id to consolidate under a deterministic config
    prompt_df = bronze_df.select(lit("abuse_consolidate@v0").alias("prompt_id"), lit(None).alias("prompt_json")).limit(
        1
    )
    candidates_df = bronze_df.crossJoin(prompt_df)

    silver_df = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_frame_abuse_consolidate.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", "prompt_id")
    )
    todo_df = (
        spark.table(SILVER_TASKS_TODO_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_frame_abuse_consolidate.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", "prompt_id")
    )
    candidates_df = candidates_df.join(silver_df, ["file_hash", "prompt_id"], "left_anti")
    candidates_df = candidates_df.join(todo_df, ["file_hash", "prompt_id"], "left_anti")

    # Gather and aggregate all frame_abuse result_ids per video into a single array
    frame_abuse_df = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_frame_abuse.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", "queue_id")
        .groupBy("file_hash")
        .agg(collect_list(col("queue_id")).alias("dependency_result_ids"))
    )
    with_dep = candidates_df.join(frame_abuse_df, ["file_hash"], "inner")

    return with_dep


def process_item(item: dict) -> ProcessingResult:
    frame_abuse_result_json = item.get("frame_abuse_result_json")
    if not frame_abuse_result_json:
        print(item.keys())
        raise ValueError("frame_abuse_result_json is None")
        return ProcessingResult(status="failed", result_json=None, error_message="frame_abuse_result_json is None")
    frame_abuse_result = VideoAbuseFrameResult.model_validate_json(frame_abuse_result_json)
    frame_results = frame_abuse_result.frame_results
    # Collect all relevant fields from frame_results in a single pass for clarity and elegance
    abuse_categories, abuse_categories_conf, abuse_categories_reasoning = [], [], []
    adversary_levels, adversary_levels_conf, adversary_levels_reasoning = [], [], []

    for frame in frame_results:
        if not frame.is_category:
            continue
        abuse_categories.append(frame.category_name)
        abuse_categories_conf.append(frame.cat_conf)
        abuse_categories_reasoning.append(frame.cat_reason)
        adversary_levels.append(frame.adv_level)
        adversary_levels_conf.append(frame.adv_conf)
        adversary_levels_reasoning.append(frame.adv_reason)

    video_profile_result = consolidate_abuse_categories(
        abuse_categories=abuse_categories,
        abuse_categories_conf=abuse_categories_conf,
        abuse_categories_reasoning=abuse_categories_reasoning,
        adversary_levels=adversary_levels,
        adversary_levels_conf=adversary_levels_conf,
        adversary_levels_reasoning=adversary_levels_reasoning,
    )
    return ProcessingResult(status="processed", result_json=video_profile_result.model_dump_json(), error_message=None)


def processor(items: list[dict]) -> list[ProcessingResult]:
    """
    Consolidate all upstream frame_abuse results per video using framework-provided upstream_parsed.
    """
    results: list[ProcessingResult] = []
    for item in items:
        parsed_upstreams = item.get("upstream_parsed") or []

        abuse_categories = []
        abuse_categories_conf = []
        abuse_categories_reasoning = []
        adversary_levels = []
        adversary_levels_conf = []
        adversary_levels_reasoning = []

        for parsed in parsed_upstreams:
            frame_abuse_result: VideoAbuseFrameResult = parsed  # already parsed by framework
            for frame in frame_abuse_result.frame_results:
                if not getattr(frame, "is_category", False):
                    continue
                abuse_categories.append(frame.category_name)
                abuse_categories_conf.append(frame.cat_conf)
                abuse_categories_reasoning.append(frame.cat_reason)
                adversary_levels.append(frame.adv_level)
                adversary_levels_conf.append(frame.adv_conf)
                adversary_levels_reasoning.append(frame.adv_reason)

        video_profile_result = consolidate_abuse_categories(
            abuse_categories=abuse_categories,
            abuse_categories_conf=abuse_categories_conf,
            abuse_categories_reasoning=abuse_categories_reasoning,
            adversary_levels=adversary_levels,
            adversary_levels_conf=adversary_levels_conf,
            adversary_levels_reasoning=adversary_levels_reasoning,
        )
        results.append(
            ProcessingResult(status="processed", result_json=video_profile_result.model_dump_json(), error_message=None)
        )

    return results


def get_video_frame_abuse_consolidate_spec() -> TaskSpec:
    return TaskSpec(
        task_key=TaskKey.video_frame_abuse_consolidate,
        media_type=MediaType.video,
        prompt_table_type=None,
        processor=processor,
        max_attempts=3,
        build_candidates_df=build_candidates_df,
    )


if __name__ == "__main__":
    # Minimal happy-path: plan -> process -> consolidate
    from constants import SILVER_TASKS_RESULTS_TABLE
    from databricks.sdk.runtime import spark
    from task_framework import build_and_enqueue_todos, consolidate_task, process_batch

    spark.sql(
        "DELETE FROM af_delivery_dev.silver_deepmind_labeling.tasks_status where task_key = 'video.frame_abuse_consolidate';"
    )
    spark.sql(
        "DELETE FROM af_delivery_dev.silver_deepmind_labeling.tasks_todo where task_key = 'video.frame_abuse_consolidate';"
    )
    spark.sql(
        "DELETE FROM af_delivery_dev.silver_deepmind_labeling.tasks_results where task_key = 'video.frame_abuse_consolidate';"
    )

    spec = get_video_frame_abuse_consolidate_spec()
    print("[video_frame_abuse_task] Planning a small batch (limit=1)...")
    batch_ids = build_and_enqueue_todos(spec, limit=10)
    print(f"[video_frame_abuse_consolidate_task] Planned {len(batch_ids)} batch(es): {batch_ids}")
    # expect 5 batches because of 5 abuse categories
    # assert len(batch_ids) == 5, f"Expected 5 batches, got {len(batch_ids)}"
    for bid in batch_ids:
        print(f"[video_frame_abuse_consolidate_task] Processing batch_id={bid}...")
        process_batch(bid, spec)
    print("[video_frame_abuse_consolidate_task] Consolidating...")
    consolidate_task(spec)
    # Optional: small visibility into results count
    results_cnt = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_frame_abuse_consolidate.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .count()
    )
    print(
        f"[video_frame_abuse_consolidate_task] Done. Current total results for task={TaskKey.video_frame_abuse_consolidate.value}: {results_cnt}"
    )
