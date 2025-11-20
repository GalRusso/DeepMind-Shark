import asyncio
import json
import logging
import traceback

import nest_asyncio
from constants import (
    LANDING_TABLE_NAME,
    PROMPT_PROFILE_TABLE_NAME,
    SILVER_TASKS_RESULTS_TABLE,
    SILVER_TASKS_TODO_TABLE,
    MediaType,
    TaskKey,
)
from databricks.sdk.runtime import dbutils, spark
from llm_orchestrator.llm_helpers import init_chat_model
from llm_orchestrator.video_des_tag import VideoDescriptionTagProcessor
from prompt_profiles_config import PromptTableType, PTVideoDesTag
from pyspark.sql import DataFrame
from pyspark.sql.functions import array, col, lit
from task_framework import ProcessingResult, TaskSpec


def build_candidates_df(limit: int | None = None) -> DataFrame:
    """
    Placeholder: cross-join videos with prompt profiles of type video_description_tag.
    Exclude already processed or enqueued rows.
    """

    bronze_df = spark.table(LANDING_TABLE_NAME).select(
        "file_hash", "mimetype", "file_size", "landing_id", "source_file"
    )
    bronze_df = bronze_df.filter("split(mimetype, '/')[0] IN ('video')")
    if limit is not None:
        bronze_df = bronze_df.orderBy("landing_id").limit(limit)

    prompts_df = (
        spark.table(PROMPT_PROFILE_TABLE_NAME)
        .where(col("type") == PromptTableType.video_description_tag.value)
        .select(
            col("id").alias("prompt_id"),
            col("version").alias("prompt_version"),
            col("prompt_hash"),
            col("content_json").alias("prompt_json"),
        )
    )

    candidates_df = bronze_df.join(prompts_df, how="cross")

    silver_df = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_des_tag.value)) & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", "prompt_id")
    )
    todo_df = (
        spark.table(SILVER_TASKS_TODO_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_des_tag.value)) & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", "prompt_id")
    )
    candidates_df = candidates_df.join(silver_df, ["file_hash", "prompt_id"], "left_anti")
    to_insert_df = candidates_df.join(todo_df, ["file_hash", "prompt_id"], "left_anti")
    to_insert_df = to_insert_df.drop("file_size", "mimetype", "prompt_hash", "prompt_version")
    # TODO: add dependency_result_ids by find frames extracted from video
    frames_idf = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_extract_frames.value))
            & (col("media_type") == lit(MediaType.video.value))
            & (col("result_json").isNotNull())
            & (col("prompt_id") == lit("video_extract_frames@ffmpeg-frames3@v0"))
        )
        .select("file_hash", col("result_id").alias("dependency_result_id"))
    )
    to_insert_df_with_deps = to_insert_df.join(frames_idf, ["file_hash"], "inner")
    to_insert_df_with_deps = to_insert_df_with_deps.withColumn(
        "dependency_result_ids", array(col("dependency_result_id"))
    )
    return to_insert_df_with_deps


def processor(items: list[dict]) -> list[ProcessingResult]:
    """
    Placeholder: return a synthetic description and empty tags for each video.
    """
    prompt_jsons = [item["prompt_json"] for item in items]
    assert len(set(prompt_jsons)) == 1, "All prompt_jsons must be the same to be batch processed"

    prompt_json = prompt_jsons[0]
    prompt = PTVideoDesTag.model_validate_json(prompt_json)
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger(__name__)

    # Use framework-enriched upstream_results to obtain frames per item
    source_files = []
    source_frames = []
    for item in items:
        source_file = item.get("source_file")
        frames = []
        for upstream in item.get("upstream_results", []) or []:
            try:
                payload = json.loads(upstream.get("result_json")) if upstream.get("result_json") else {}
                frames = payload.get("frames", [])
                if frames:
                    break
            except (json.JSONDecodeError, AttributeError):
                frames = []
        source_files.append(source_file)
        source_frames.append(frames)

    llms = []
    for model in prompt.models:
        llms.append(init_chat_model(model))

    frame_llms = []
    for model in prompt.frame_models:
        frame_llms.append(init_chat_model(model))

    processor = VideoDescriptionTagProcessor(
        prompt=prompt,
        llms=llms,
        fallback_llms=frame_llms,
        #logger=logger,
        gemini_api_key=dbutils.secrets.get(scope="Gal Russo Keys", key="Gemini Api key CT"),
    )

    results: list[ProcessingResult] = []
    try:
        nest_asyncio.apply()
        batch_results = asyncio.run(processor.abatch(source_files, source_frames))
        for r in batch_results:
            results.append(ProcessingResult(status="processed", result_json=r.model_dump_json(), error_message=None))
    except Exception as e:
        tb_str = traceback.format_exc()
        shared_error = f"{e!s}\ntraceback:\n{tb_str[:1000]}"
        for _ in range(len(source_files)):
            results.append(ProcessingResult(status="failed", result_json=None, error_message=shared_error))

    return results


def get_video_des_tag_spec() -> TaskSpec:
    return TaskSpec(
        task_key=TaskKey.video_des_tag,
        media_type=MediaType.video,
        prompt_table_type=PromptTableType.video_description_tag,
        processor=processor,
        max_attempts=3,
        build_candidates_df=build_candidates_df,
    )


if __name__ == "__main__":
    # Minimal happy-path: plan -> process -> consolidate
    from databricks.sdk.runtime import spark
    from task_framework import build_and_enqueue_todos, consolidate_task, process_batch

    spark.sql("DELETE FROM af_delivery_dev.silver_deepmind_labeling.tasks_status where task_key = 'video.des_tag';")
    spark.sql("DELETE FROM af_delivery_dev.silver_deepmind_labeling.tasks_todo where task_key = 'video.des_tag';")
    spark.sql("DELETE FROM af_delivery_dev.silver_deepmind_labeling.tasks_results where task_key = 'video.des_tag';")

    spec = get_video_des_tag_spec()
    print("[video_des_tag_task] Planning a small batch (limit=1)...")
    batch_ids = build_and_enqueue_todos(spec, limit=1)
    print(f"[video_des_tag_task] Planned {len(batch_ids)} batch(es): {batch_ids}")
    assert len(batch_ids) == 1, f"Expected 1 batch, got {len(batch_ids)}"
    for bid in batch_ids:
        print(f"[video_des_tag_task] Processing batch_id={bid}...")
        process_batch(bid, spec)
    print("[video_des_tag_task] Consolidating...")
    consolidate_task(spec)
    # Optional: small visibility into results count
    results_cnt = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_des_tag.value)) & (col("media_type") == lit(MediaType.video.value))
        )
        .count()
    )
    print(f"[video_des_tag_task] Done. Current total results for task={TaskKey.video_des_tag.value}: {results_cnt}")
