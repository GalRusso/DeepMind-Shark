import asyncio

import nest_asyncio
from constants import (
    LANDING_TABLE_NAME,
    PROMPT_PROFILE_TABLE_NAME,
    SILVER_TASKS_RESULTS_TABLE,
    SILVER_TASKS_TODO_TABLE,
    MediaType,
    TaskKey,
)
from databricks.sdk.runtime import spark
from llm_orchestrator.audio_translate import AudioTranslate
from llm_orchestrator.llm_helpers import init_chat_model
from models import MaybeAudioTranscribeResult
from prompt_profiles_config import PromptTableType, PTTextTranslation
from pyspark.sql import DataFrame
from pyspark.sql.functions import array, col, lit, xxhash64
from task_framework import ProcessingResult, TaskSpec


def build_candidates_df(limit: int | None = None) -> DataFrame:
    """
    Placeholder: require upstream transcription and enqueue one translation per video.
    """
    bronze_df = (
        spark.table(LANDING_TABLE_NAME)
        .select("file_hash", "mimetype", "file_size", "landing_id", "source_file")
        .filter("split(mimetype, '/')[0] IN ('video')")
    )
    if limit is not None:
        bronze_df = bronze_df.orderBy("landing_id").limit(limit)

    prompt_df = (
        spark.table(PROMPT_PROFILE_TABLE_NAME)
        .where(col("type") == PromptTableType.text_translation.value)
        .select(
            col("id").alias("prompt_id"),
            col("version").alias("prompt_version"),
            col("prompt_hash"),
            col("content_json").alias("prompt_json"),
        )
    )
    candidates_df = bronze_df.crossJoin(prompt_df)

    silver_df = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_audio_translate.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", "prompt_id")
    )
    todo_df = (
        spark.table(SILVER_TASKS_TODO_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_audio_translate.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", "prompt_id")
    )
    candidates_df = candidates_df.join(silver_df, ["file_hash", "prompt_id"], "left_anti")
    candidates_df = candidates_df.join(todo_df, ["file_hash", "prompt_id"], "left_anti")

    upstream_df = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_audio_transcribe.value))
            & (col("media_type") == lit(MediaType.video.value))
            & (col("prompt_id") == lit("whisper-1"))
        )
        .select("file_hash", col("prompt_id").alias("prompt_transcribe_id"))
    )
    to_insert_df = candidates_df.join(upstream_df, ["file_hash"], "inner")
    to_insert_df = to_insert_df.withColumn(
        "dep_transcribe_id",
        xxhash64(
            lit(TaskKey.video_audio_transcribe.value),
            lit(MediaType.video.value),
            col("file_hash"),
            col("prompt_transcribe_id"),
        ),
    )
    to_insert_df = to_insert_df.drop("file_size", "mimetype").withColumn(
        "dependency_result_ids", array(col("dep_transcribe_id"))
    )
    return to_insert_df


def processor(items: list[dict]) -> list[ProcessingResult]:
    """
    Placeholder: emit an empty translated text.
    """
    prompt_jsons = [item["prompt_json"] for item in items]
    assert len(set(prompt_jsons)) == 1, "All prompt_jsons must be the same to be batch processed"

    prompt_json = prompt_jsons[0]
    prompt = PTTextTranslation.model_validate_json(prompt_json)

    # Use framework-enriched upstream_parsed (AutioTranscribeResult) to populate transcriptions
    for item in items:
        transcription_lines: list[str] | None = None
        for upstream in item.get("upstream_parsed", []) or []:
            if isinstance(upstream, MaybeAudioTranscribeResult):
                if upstream.result is None:
                    continue
                if upstream.result.segments:
                    transcription_lines = [seg.text for seg in upstream.result.segments]
                elif upstream.result.text:
                    transcription_lines = [upstream.result.text]
                break
        item["_transcription_lines"] = transcription_lines

    llms = []
    for model in prompt.models:
        llms.append(init_chat_model(model))

    # sparse transcribe
    translator = AudioTranslate(prompt=prompt, llms=llms)

    transcriptions = []
    for item in items:
        transcriptions.append(item.get("_transcription_lines"))

    nest_asyncio.apply()
    translate_results = asyncio.run(translator.abatch([t for t in transcriptions if t is not None]))

    results: list[ProcessingResult] = []
    result_iter = iter(translate_results)
    for transcription in transcriptions:
        if transcription is None:
            results.append(ProcessingResult(status="processed", result_json="{}", error_message=None))
        else:
            r = next(result_iter)
            results.append(ProcessingResult(status="processed", result_json=r.model_dump_json(), error_message=None))
    return results


def get_video_audio_translate_spec() -> TaskSpec:
    return TaskSpec(
        task_key=TaskKey.video_audio_translate,
        media_type=MediaType.video,
        prompt_table_type=PromptTableType.text_translation,
        processor=processor,
        max_attempts=3,
        build_candidates_df=build_candidates_df,
    )


if __name__ == "__main__":
    # Minimal happy-path: plan -> process -> consolidate
    from databricks.sdk.runtime import spark
    from task_framework import build_and_enqueue_todos, consolidate_task, process_batch

    spark.sql("DELETE FROM af_delivery_dev.silver_labeling.tasks_status where task_key = 'video.audio_translate';")
    spark.sql("DELETE FROM af_delivery_dev.silver_labeling.tasks_todo where task_key = 'video.audio_translate';")
    spark.sql("DELETE FROM af_delivery_dev.silver_labeling.tasks_results where task_key = 'video.audio_translate';")

    spec = get_video_audio_translate_spec()
    print("[video_audio_translate_task] Planning a small batch (limit=1)...")
    batch_ids = build_and_enqueue_todos(spec, limit=10)
    print(f"[video_audio_transcribe_task] Planned {len(batch_ids)} batch(es): {batch_ids}")
    assert len(batch_ids) == 1, f"Expected 1 batch, got {len(batch_ids)}"
    for bid in batch_ids:
        print(f"[video_audio_translate_task] Processing batch_id={bid}...")
        process_batch(bid, spec)
    print("[video_audio_translate_task] Consolidating...")
    consolidate_task(spec)
    # Optional: small visibility into results count
    results_cnt = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_audio_translate.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .count()
    )
    print(
        f"[video_audio_translate_task] Done. Current total results for task={TaskKey.video_audio_translate.value}: {results_cnt}"
    )
