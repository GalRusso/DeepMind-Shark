import asyncio

import nest_asyncio
from constants import LANDING_TABLE_NAME, SILVER_TASKS_RESULTS_TABLE, SILVER_TASKS_TODO_TABLE, MediaType, TaskKey
from databricks.sdk.runtime import dbutils, spark
from llm_orchestrator.audio_transcribe import AudioTranscribe
from models import MaybeAudioTranscribeResult
from pyspark.sql import DataFrame
from pyspark.sql.functions import array, col, lit, xxhash64
from task_framework import ProcessingResult, TaskSpec
from tasks.video_extract_audio_task import AudioResult


def build_candidates_df(limit: int | None = None) -> DataFrame:
    """
    Placeholder: require upstream video.extract_audio and enqueue one transcription per video.
    """
    from constants import SILVER_TASKS_RESULTS_TABLE

    bronze_df = (
        spark.table(LANDING_TABLE_NAME)
        .select("file_hash", "mimetype", "file_size", "landing_id", "source_file")
        .filter("split(mimetype, '/')[0] IN ('video')")
    )
    if limit is not None:
        bronze_df = bronze_df.orderBy("landing_id").limit(limit)

    prompt_df = bronze_df.select(lit("whisper-1").alias("prompt_id"), lit(None).alias("prompt_json")).limit(1)
    candidates_df = bronze_df.crossJoin(prompt_df)

    silver_df = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_audio_transcribe.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", "prompt_id")
    )
    todo_df = (
        spark.table(SILVER_TASKS_TODO_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_audio_transcribe.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", "prompt_id")
    )
    candidates_df = candidates_df.join(silver_df, ["file_hash", "prompt_id"], "left_anti")
    candidates_df = candidates_df.join(todo_df, ["file_hash", "prompt_id"], "left_anti")

    upstream_df = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_extract_audio.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", col("prompt_id").alias("prompt_extract_audio_id"))
    )
    to_insert_df = candidates_df.join(upstream_df, ["file_hash"], "inner")
    to_insert_df = to_insert_df.withColumn(
        "dep_audio_id",
        xxhash64(
            lit(TaskKey.video_extract_audio.value),
            lit(MediaType.video.value),
            col("file_hash"),
            col("prompt_extract_audio_id"),
        ),
    )
    to_insert_df = to_insert_df.drop("file_size", "mimetype").withColumn(
        "dependency_result_ids", array(col("dep_audio_id"))
    )

    return to_insert_df


def processor(items: list[dict]) -> list[ProcessingResult]:
    """
    Transcribe the audio and return the transcript in result_json.
    """
    # Use framework-enriched upstream_results to populate audio_file
    # sparse audio paths
    audio_paths = []
    for item in items:
        audio_path = None
        for upstream in item.get("upstream_parsed", []) or []:
            if isinstance(upstream, AudioResult):
                audio_path = upstream.audio_path
                break
        audio_paths.append(audio_path)

    assert len({item["prompt_id"] for item in items}) == 1, "All items must have the same prompt_id"

    prompt_id = items[0]["prompt_id"]  # whisper-1

    transcriber = AudioTranscribe(
        model=prompt_id,
        openai_key=dbutils.secrets.get(scope="datn_labeling", key="openai_api_key"),
    )
    results: list[ProcessingResult] = []
    nest_asyncio.apply()
    batch_results = asyncio.run(transcriber.abatch([path for path in audio_paths if path is not None]))

    result_iter = iter(batch_results)
    for audio_path in audio_paths:
        if audio_path is None:
            results.append(
                ProcessingResult(
                    status="processed",
                    result_json=MaybeAudioTranscribeResult(
                        result=None, reason="no audio for this file"
                    ).model_dump_json(),
                    error_message=None,
                )
            )
        else:
            r = next(result_iter)
            results.append(
                ProcessingResult(
                    status="processed",
                    result_json=MaybeAudioTranscribeResult(result=r).model_dump_json(),
                    error_message=None,
                )
            )

    return results


def get_video_audio_transcribe_spec() -> TaskSpec:
    return TaskSpec(
        task_key=TaskKey.video_audio_transcribe,
        media_type=MediaType.video,
        prompt_table_type=None,
        processor=processor,
        max_attempts=3,
        build_candidates_df=build_candidates_df,
    )


if __name__ == "__main__":
    # Minimal happy-path: plan -> process -> consolidate
    from databricks.sdk.runtime import spark
    from task_framework import build_and_enqueue_todos, consolidate_task, process_batch

    spark.sql("DELETE FROM af_delivery_dev.silver_labeling.tasks_status where task_key = 'video.audio_transcribe';")
    spark.sql("DELETE FROM af_delivery_dev.silver_labeling.tasks_todo where task_key = 'video.audio_transcribe';")
    spark.sql("DELETE FROM af_delivery_dev.silver_labeling.tasks_results where task_key = 'video.audio_transcribe';")

    spec = get_video_audio_transcribe_spec()
    print("[video_audio_transcribe_task] Planning a small batch (limit=1)...")
    batch_ids = build_and_enqueue_todos(spec, limit=10)
    print(f"[video_audio_transcribe_task] Planned {len(batch_ids)} batch(es): {batch_ids}")
    assert len(batch_ids) == 1, f"Expected 1 batch, got {len(batch_ids)}"
    for bid in batch_ids:
        print(f"[video_audio_transcribe_task] Processing batch_id={bid}...")
        process_batch(bid, spec)
    print("[video_audio_transcribe_task] Consolidating...")
    consolidate_task(spec)
    # Optional: small visibility into results count
    results_cnt = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_audio_transcribe.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .count()
    )
    print(
        f"[video_audio_transcribe_task] Done. Current total results for task={TaskKey.video_audio_transcribe.value}: {results_cnt}"
    )
