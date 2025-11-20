import datetime
import traceback
from collections import defaultdict

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
from llm_orchestrator.video_abuse_profiler import VideoAbuseFrameResult
from llm_orchestrator.video_des_tag import VideoDescriptionTagResult
from prompt_profiles_config import ImageAbuseCategory, PromptTableType, PTImageDesTag, VideoAbuseCategory
from pyspark.sql import DataFrame
from pyspark.sql.functions import array, col, concat, floor, from_json, lit, row_number
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window
from task_framework import ProcessingResult, TaskSpec
from tasks.video_extract_frames_task import VideoExtractFramesResult


def build_candidates_df(limit: int | None = None) -> DataFrame:
    """
    Depend on both video.extract_frames and video.des_tag.
    Do not explode frames; carry upstream result_ids as dependency_result_ids.
    """

    bronze_df = spark.table(LANDING_TABLE_NAME).select(
        "file_hash", "mimetype", "file_size", "landing_id", "source_file"
    )
    bronze_df = bronze_df.filter("split(mimetype, '/')[0] IN ('video')")
    if limit is not None:
        bronze_df = bronze_df.orderBy("landing_id").limit(limit)

    prompts_df = (
        spark.table(PROMPT_PROFILE_TABLE_NAME)
        .where(col("type") == PromptTableType.video_abuse_category.value)
        .select(
            col("id").alias("prompt_id"),
            col("version").alias("prompt_version"),
            col("prompt_hash"),
            col("content_json").alias("prompt_json"),
        )
    )

    # Match abuse prompt to the corresponding video description/tag prompt via template hash
    schema = StructType([StructField("pt_video_desc_tag_hash", StringType())])
    prompts_with_hash_df = (
        prompts_df.withColumn("json_struct", from_json("prompt_json", schema))
        .withColumn("pt_video_desc_tag_hash", col("json_struct.pt_video_desc_tag_hash"))
        .drop("json_struct")
    )

    des_tag_prompt_df = (
        spark.table(PROMPT_PROFILE_TABLE_NAME)
        .where(col("type") == PromptTableType.video_description_tag.value)
        .select(col("id").alias("prompt_des_tag_id"), col("prompt_hash").alias("pt_video_desc_tag_hash"))
    )

    prompts_with_deps_df = prompts_with_hash_df.join(des_tag_prompt_df, ["pt_video_desc_tag_hash"], "inner")

    candidates_df = bronze_df.join(prompts_with_deps_df, how="cross")

    # Exclude already processed/enqueued
    silver_df = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_frame_abuse.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", "prompt_id")
    )
    todo_df = (
        spark.table(SILVER_TASKS_TODO_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_frame_abuse.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", "prompt_id")
    )
    candidates_df = candidates_df.join(silver_df, ["file_hash", "prompt_id"], "left_anti")
    candidates_df = candidates_df.join(todo_df, ["file_hash", "prompt_id"], "left_anti")

    # Require extract_frames upstream result_ids
    frames_upstream = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_extract_frames.value))
            & (col("media_type") == lit(MediaType.video.value))
            & (col("prompt_id") == lit("video_extract_frames@ffmpeg-frames3@v0"))
        )
        .select("file_hash", col("result_id").alias("frames_result_id"))
    )

    # Require description/tag upstream result_ids
    des_tag_upstream = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_des_tag.value)) & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", col("prompt_id").alias("prompt_des_tag_id"), col("result_id").alias("des_tag_result_id"))
    )

    to_insert_df = candidates_df.join(frames_upstream, ["file_hash"], "inner").join(
        des_tag_upstream, ["file_hash", "prompt_des_tag_id"], "inner"
    )

    to_insert_df = to_insert_df.drop("file_size", "mimetype", "prompt_hash", "prompt_version").withColumn(
        "dependency_result_ids", array(col("frames_result_id"), col("des_tag_result_id"))
    )

    return to_insert_df


def processor(items: list[dict]) -> list[ProcessingResult]:
    """
    Prefetches upstream frames and des/tag results
    Form a batch for frame abuse profiling
    Run the profiler
    Gather the results for each frame
    """
    if len(items) == 0:
        return []

    assert len({i["prompt_json"] for i in items}) == 1, "All prompt_jsons must be the same to be batch processed"
    prompt_json = items[0]["prompt_json"]
    pt_video_abuse_category = VideoAbuseCategory.model_validate_json(prompt_json)

    llms = []
    for model in pt_video_abuse_category.models:
        llms.append(init_chat_model(model))

    # Construct the batch input enriched with upstream info
    source_files = []
    descriptions: list[str | None] = []
    taggs: list[list[str] | None] = []
    source_frames: list[list[str]] = []
    for item in items:
        source_file = item.get("source_file")
        source_files.append(source_file)
        _frame_paths = []
        _desc = None
        _tags = None
        # Prefer framework-enriched typed upstream_parsed
        for upstream in item.get("upstream_parsed", []) or []:
            if isinstance(upstream, VideoExtractFramesResult):
                _frame_paths = upstream.frames or []
            elif isinstance(upstream, VideoDescriptionTagResult):
                _desc = _desc or upstream.desc_result.description
                _tags = _tags or upstream.tags_result.tags
        source_frames.append(_frame_paths)
        descriptions.append(_desc)
        taggs.append(_tags)

    # Flat the source_frames, help working with ImageAbuseProfiler
    f_source_files = []
    f_descriptions: list[str | None] = []
    f_taggs: list[list[str] | None] = []
    f_source_frames: list[str] = []
    for _file, _desc, _tags, _frames in zip(source_files, descriptions, taggs, source_frames, strict=True):
        for _frame in _frames:
            f_source_files.append(_file)
            f_descriptions.append(_desc)
            f_taggs.append(_tags)
            f_source_frames.append(_frame)

    # Actually run the profiler, in batch
    profiler = ImageAbuseProfiler(
        abuse_category=ImageAbuseCategory(
            version=pt_video_abuse_category.version,
            models=pt_video_abuse_category.models,
            category_name=pt_video_abuse_category.category_name,
            abuse_category_prompt_template=pt_video_abuse_category.abuse_category_prompt_template,
            adversary_levels_prompt_template=pt_video_abuse_category.adversary_levels_prompt_template,
            category_definition=pt_video_abuse_category.category_definition,
            adversary_levels=pt_video_abuse_category.adversary_levels,
            pt_image_desc_tag=PTImageDesTag(
                version=pt_video_abuse_category.pt_video_desc_tag.version,
                models=pt_video_abuse_category.pt_video_desc_tag.models,
                description_prompt_template=pt_video_abuse_category.pt_video_desc_tag.pt_video_desc,
                tag_prompt_template=pt_video_abuse_category.pt_video_desc_tag.pt_video_tags,
            ),
        ),
        llms=llms,
    )
    try:
        frame_batch_results = profiler.batch(
            file_paths=f_source_frames,
            descriptions=f_descriptions,
            tagss=f_taggs,
        )
    except Exception as e:
        tb_str = traceback.format_exc()
        shared_error = f"{e!s}\ntraceback:\n{tb_str[:1000]}"
        video_results = [ProcessingResult(status="failed", result_json=None, error_message=shared_error)] * len(
            source_files
        )
        return video_results

    # group by source_file again
    grouped_by_video = defaultdict(list)
    for _video_file, _frame_results in zip(f_source_files, frame_batch_results, strict=False):
        grouped_by_video[_video_file].append(_frame_results)

    frame_results: list[VideoAbuseFrameResult] = []
    for _video_file, _frame_results in grouped_by_video.items():
        frame_results.append(VideoAbuseFrameResult(video_file=_video_file, frame_results=_frame_results))

    results = []
    for _frame_result in frame_results:
        results.append(
            ProcessingResult(status="processed", result_json=_frame_result.model_dump_json(), error_message=None)
        )
    return results


def build_batch_strategy(candidates_df: DataFrame) -> tuple[DataFrame, list[str]]:
    """
    Batch by prompt_id to ensure each batch contains homogeneous prompts.
    Assigns a run-scoped batch_id per chunk using a UTC timestamp run_key.
    Returns (DataFrame with batch_id, list of distinct batch_ids).
    """
    DEFAULT_BATCH_SIZE = 16

    run_key = datetime.datetime.now(datetime.UTC).isoformat()

    w = Window.partitionBy("prompt_id").orderBy("landing_id", "file_hash", "prompt_id")
    df_with_index = candidates_df.withColumn("_rn", row_number().over(w))
    df_with_batch_index = df_with_index.withColumn(
        "_batch_index", floor((col("_rn") - lit(1)) / lit(DEFAULT_BATCH_SIZE))
    )
    df_with_batch_id = df_with_batch_index.withColumn(
        "batch_id",
        concat(lit(run_key), lit("-"), col("prompt_id").substr(1, 8), lit("-"), col("_batch_index").cast("string")),
    ).drop("_rn", "_batch_index")

    batch_ids = [r.batch_id for r in df_with_batch_id.select("batch_id").distinct().collect()]
    return df_with_batch_id, batch_ids


def get_video_frame_abuse_spec() -> TaskSpec:
    return TaskSpec(
        task_key=TaskKey.video_frame_abuse,
        media_type=MediaType.video,
        prompt_table_type=PromptTableType.video_abuse_category,
        processor=processor,
        max_attempts=3,
        build_candidates_df=build_candidates_df,
        batch_strategy=build_batch_strategy,
    )


if __name__ == "__main__":
    # Minimal happy-path: plan -> process -> consolidate
    from databricks.sdk.runtime import spark
    from task_framework import build_and_enqueue_todos, consolidate_task, process_batch

    spark.sql("DELETE FROM af_delivery_dev.silver_labeling.tasks_status where task_key = 'video.frame_abuse';")
    spark.sql("DELETE FROM af_delivery_dev.silver_labeling.tasks_todo where task_key = 'video.frame_abuse';")
    spark.sql("DELETE FROM af_delivery_dev.silver_labeling.tasks_results where task_key = 'video.frame_abuse';")

    spec = get_video_frame_abuse_spec()
    print("[video_frame_abuse_task] Planning a small batch (limit=1)...")
    batch_ids = build_and_enqueue_todos(spec, limit=1)
    print(f"[video_frame_abuse_task] Planned {len(batch_ids)} batch(es): {batch_ids}")
    # expect 5 batches because of 5 abuse categories
    assert len(batch_ids) == 5, f"Expected 5 batches, got {len(batch_ids)}"
    for bid in batch_ids:
        print(f"[video_frame_abuse_task] Processing batch_id={bid}...")
        process_batch(bid, spec)
    print("[video_frame_abuse_task] Consolidating...")
    consolidate_task(spec)
    # Optional: small visibility into results count
    results_cnt = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_frame_abuse.value))
            & (col("media_type") == lit(MediaType.video.value))
        )
        .count()
    )
    print(
        f"[video_frame_abuse_task] Done. Current total results for task={TaskKey.video_frame_abuse.value}: {results_cnt}"
    )
