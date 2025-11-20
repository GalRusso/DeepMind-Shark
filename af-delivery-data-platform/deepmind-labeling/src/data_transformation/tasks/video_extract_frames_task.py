from constants import (
    LANDING_TABLE_NAME,
    SILVER_TASKS_RESULTS_TABLE,
    SILVER_TASKS_TODO_TABLE,
    MediaType,
    TaskKey,
    silver_dir_path,
)
from databricks.sdk.runtime import spark
from llm_orchestrator.video_helpers import extract_frames, get_video_width_height
from models import VideoExtractFramesResult
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, struct, to_json
from task_framework import ProcessingResult, TaskSpec, all_batch_strategy


def build_candidates_df(limit: int | None = None) -> DataFrame:
    """
    Placeholder: select videos from landing and attach a single prompt seed.
    Later: add prompt profile for frame extraction parameters (fps/stride/resize).
    """

    bronze_df = spark.table(LANDING_TABLE_NAME).select(
        "file_hash", "mimetype", "file_size", "landing_id", "source_file"
    )
    bronze_df = bronze_df.filter("split(mimetype, '/')[0] IN ('video')")
    if limit is not None:
        bronze_df = bronze_df.orderBy("landing_id")
        bronze_df = bronze_df.limit(limit)

    # Minimal deterministic prompt/config for extraction
    prompt_id_literal = lit("video_extract_frames@ffmpeg-frames3@v0")
    prompt_json_literal = to_json(struct(lit(1).alias("version")))
    prompt_seed_df = bronze_df.select(
        prompt_id_literal.alias("prompt_id"), prompt_json_literal.alias("prompt_json")
    ).limit(1)

    candidates_df = bronze_df.crossJoin(prompt_seed_df)

    # Exclude already processed or enqueued
    silver_df = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_extract_frames.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", "prompt_id")
    )
    todo_df = (
        spark.table(SILVER_TASKS_TODO_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_extract_frames.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", "prompt_id")
    )
    candidates_df = candidates_df.join(silver_df, ["file_hash", "prompt_id"], "left_anti")
    to_insert_df = candidates_df.join(todo_df, ["file_hash", "prompt_id"], "left_anti")

    return to_insert_df.drop("file_size", "mimetype")


def processor(items: list[dict]) -> list[ProcessingResult]:
    """
    Placeholder processor. Does not actually extract frames.
    Returns a minimal JSON structure with an empty frames list.
    """
    assert len(items) == 1, "Only one item is supported for video frame extraction"
    item = items[0]

    source_file = item["source_file"]
    prompt_json = item["prompt_json"]
    prompt_id = item["prompt_id"]
    assert prompt_id == "video_extract_frames@ffmpeg-frames3@v0", (
        "Only video extract using ffmpeg, 3 frames are supported"
    )

    results: list[ProcessingResult] = []
    for _ in items:
        frame_paths, duration = extract_frames(source_file, output_dir=silver_dir_path())
        width, height = get_video_width_height(source_file)
        frame_extraction_result = VideoExtractFramesResult(
            frames=frame_paths or [], duration=duration, width=width, height=height
        )
        results.append(
            ProcessingResult(
                status="processed", result_json=frame_extraction_result.model_dump_json(), error_message=None
            )
        )
    return results


def get_video_extract_frames_spec() -> TaskSpec:
    return TaskSpec(
        task_key=TaskKey.video_extract_frames,
        media_type=MediaType.video,
        prompt_table_type=None,
        processor=processor,
        max_attempts=3,
        build_candidates_df=build_candidates_df,
        batch_strategy=all_batch_strategy,
    )
