from constants import (
    LANDING_TABLE_NAME,
    SILVER_TASKS_RESULTS_TABLE,
    SILVER_TASKS_TODO_TABLE,
    MediaType,
    TaskKey,
    silver_dir_path,
)
from databricks.sdk.runtime import spark
from llm_orchestrator.video_helpers import extract_audio_from_video
from models import AudioResult
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, struct, to_json
from task_framework import ProcessingResult, TaskSpec, all_batch_strategy


def build_candidates_df(limit: int | None = None) -> DataFrame:
    """
    Placeholder: one task per video with a fixed prompt/config.
    """

    bronze_df = (
        spark.table(LANDING_TABLE_NAME)
        .select("file_hash", "mimetype", "file_size", "landing_id", "source_file")
        .filter("split(mimetype, '/')[0] IN ('video')")
    )
    if limit is not None:
        bronze_df = bronze_df.orderBy("landing_id").limit(limit)

    prompt_seed_df = bronze_df.select(
        lit("video_extract_audio@v0").alias("prompt_id"), to_json(struct(lit(1).alias("version"))).alias("prompt_json")
    ).limit(1)
    candidates_df = bronze_df.crossJoin(prompt_seed_df)

    silver_df = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_extract_audio.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", "prompt_id")
    )
    todo_df = (
        spark.table(SILVER_TASKS_TODO_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_extract_audio.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", "prompt_id")
    )
    candidates_df = candidates_df.join(silver_df, ["file_hash", "prompt_id"], "left_anti")
    to_insert_df = candidates_df.join(todo_df, ["file_hash", "prompt_id"], "left_anti")

    return to_insert_df.drop("file_size", "mimetype")


def processor(items: list[dict]) -> list[ProcessingResult]:
    """
    Extract audio from video and return the path in result_json.
    """
    assert len(items) == 1, "Only one item is supported for video audio extraction"
    item = items[0]

    source_file = item["source_file"]
    prompt_json = item["prompt_json"]
    prompt_id = item["prompt_id"]
    assert prompt_id == "video_extract_audio@v0", "Only audio extracttion using ffmpeg"

    results: list[ProcessingResult] = []
    for _ in items:
        audio_path = extract_audio_from_video(source_file, outdir=silver_dir_path())
        result = AudioResult(audio_path=audio_path)
        results.append(ProcessingResult(status="processed", result_json=result.model_dump_json(), error_message=None))
    return results


def get_video_extract_audio_spec() -> TaskSpec:
    return TaskSpec(
        task_key=TaskKey.video_extract_audio,
        media_type=MediaType.video,
        prompt_table_type=None,
        processor=processor,
        max_attempts=3,
        build_candidates_df=build_candidates_df,
        batch_strategy=all_batch_strategy,
    )


if __name__ == "__main__":
    # Minimal happy-path: plan -> process -> consolidate
    from databricks.sdk.runtime import spark
    from task_framework import build_and_enqueue_todos, consolidate_task, process_batch

    spark.sql("DELETE FROM af_delivery_dev.silver_deepmind_labeling.tasks_status where task_key = 'video.extract_audio';")
    spark.sql("DELETE FROM af_delivery_dev.silver_deepmind_labeling.tasks_todo where task_key = 'video.extract_audio';")
    spark.sql("DELETE FROM af_delivery_dev.silver_deepmind_labeling.tasks_results where task_key = 'video.extract_audio';")

    spec = get_video_extract_audio_spec()
    print("[video_extract_audio_task] Planning a small batch (limit=2)...")
    batch_ids = build_and_enqueue_todos(spec, limit=2)
    print(f"[video_extract_audio_task] Planned {len(batch_ids)} batch(es): {batch_ids}")
    # expect 5 batches because of 5 audio categories
    assert len(batch_ids) == 2, f"Expected 2 batches, got {len(batch_ids)}"
    for bid in batch_ids:
        print(f"[video_extract_audio_task] Processing batch_id={bid}...")
        process_batch(bid, spec)
    print("[video_extract_audio_task] Consolidating...")
    consolidate_task(spec)
    # Optional: small visibility into results count
    results_cnt = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_extract_audio.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .count()
    )
    print(
        f"[video_extract_audio_task] Done. Current total results for task={TaskKey.video_extract_audio.value}: {results_cnt}"
    )
