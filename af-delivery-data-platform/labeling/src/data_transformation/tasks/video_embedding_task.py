import json
import traceback
from typing import cast

import numpy as np
from constants import MediaType, TaskKey
from databricks.sdk.runtime import spark
from llm_orchestrator.helpers import merge_description_and_tags
from llm_orchestrator.image_embedding import CLIPClient
from llm_orchestrator.video_des_tag import VideoDescriptionTagResult
from models import VideoEmbeddingResult
from pyspark.sql import DataFrame
from pyspark.sql.functions import array, col, lit, struct, to_json, xxhash64
from pyspark.sql.functions import min as sql_min
from task_framework import ProcessingResult, TaskSpec
from tasks.video_extract_frames_task import VideoExtractFramesResult


def build_candidates_df(limit: int | None = None) -> DataFrame:
    """
    Build candidates for video embedding.

    - Videos come from LANDING_TABLE_NAME
    - Require upstream video.extract_frames (fixed prompt) and video.des_tag (choose deterministic prompt per video)
    - Use a deterministic prompt_id/prompt_json to identify the embedding configuration
    - Compute dependency_result_ids pointing to upstream frames and des_tag result_ids
    """
    from constants import LANDING_TABLE_NAME, SILVER_TASKS_RESULTS_TABLE, SILVER_TASKS_TODO_TABLE

    bronze_df = (
        spark.table(LANDING_TABLE_NAME)
        .select("file_hash", "mimetype", "file_size", "landing_id", "source_file")
        .filter("split(mimetype, '/')[0] IN ('video')")
    )
    if limit is not None:
        bronze_df = bronze_df.orderBy("landing_id").limit(limit)

    # Deterministic CLIP config (reuse image embedding schema)
    prompt_id_literal = lit("clip_vit_b_32@openai")
    prompt_json_literal = to_json(struct(lit("ViT-B-32").alias("model_name"), lit("openai").alias("pretrained")))
    prompt_seed_df = bronze_df.select(
        prompt_id_literal.alias("prompt_id"), prompt_json_literal.alias("prompt_json")
    ).limit(1)

    candidates_df = bronze_df.crossJoin(prompt_seed_df)

    # Exclude already processed and already enqueued for this task
    silver_df = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_embedding.value)) & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", "prompt_id")
    )
    todo_df = (
        spark.table(SILVER_TASKS_TODO_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_embedding.value)) & (col("media_type") == lit(MediaType.video.value))
        )
        .select("file_hash", "prompt_id")
    )
    candidates_df = candidates_df.join(silver_df, ["file_hash", "prompt_id"], "left_anti")
    candidates_df = candidates_df.join(todo_df, ["file_hash", "prompt_id"], "left_anti")

    # Require upstream successful extract_frames (fixed prompt)
    frames_prompt_id_literal = lit("video_extract_frames@ffmpeg-frames3@v0")
    frames_upstream = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_extract_frames.value))
            & (col("media_type") == lit(MediaType.video.value))
            & (col("result_json").isNotNull())
            & (col("prompt_id") == frames_prompt_id_literal)
        )
        .select("file_hash")
        .distinct()
    )

    # Require upstream successful description/tag; choose a deterministic prompt per video (min prompt_id)
    des_tag_candidates = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_des_tag.value))
            & (col("media_type") == lit(MediaType.video.value))
            & (col("result_json").isNotNull())
        )
        .select("file_hash", col("prompt_id").alias("prompt_des_tag_id"))
    )
    des_tag_per_video = des_tag_candidates.groupBy("file_hash").agg(
        sql_min("prompt_des_tag_id").alias("prompt_des_tag_id")
    )

    base_with_upstreams = candidates_df.join(frames_upstream, ["file_hash"], "inner").join(
        des_tag_per_video, ["file_hash"], "inner"
    )

    # Compute dependency_result_ids deterministically to match upstream result_id
    to_insert_df = base_with_upstreams.withColumn(
        "dep_frames_id",
        xxhash64(
            lit(TaskKey.video_extract_frames.value),
            lit(MediaType.video.value),
            col("file_hash"),
            frames_prompt_id_literal,
        ),
    ).withColumn(
        "dep_des_tag_id",
        xxhash64(
            lit(TaskKey.video_des_tag.value),
            lit(MediaType.video.value),
            col("file_hash"),
            col("prompt_des_tag_id"),
        ),
    )

    to_insert_df = to_insert_df.drop("file_size", "mimetype").withColumn(
        "dependency_result_ids", array(col("dep_frames_id"), col("dep_des_tag_id"))
    )

    return to_insert_df


def process_one(item: dict, clip: CLIPClient, model_name: str, pretrained: str) -> ProcessingResult:
    frame_paths: list[str] = []
    description: str | None = None
    tags: list[str] | None = None
    for upstream in item.get("upstream_parsed", []) or []:
        if isinstance(upstream, VideoExtractFramesResult):
            frame_paths = upstream.frames or []
        elif isinstance(upstream, VideoDescriptionTagResult):
            description = getattr(upstream.desc_result, "description", None)
            # tags_result may have attribute 'tags'
            tags = getattr(upstream.tags_result, "tags", None)

    # Compute frame-only embeddings and average
    frame_embeddings: list[list[float] | None] = []
    for frame_path in frame_paths:
        frame_embeddings.append(clip.embed_image(frame_path))
    frame_avg = CLIPClient.average_embeddings(frame_embeddings)

    # Compute text-only embedding
    text_embedding: list[float] | None = None
    merged_text = merge_description_and_tags(description, tags)
    text_embedding = clip.embed_text(merged_text)

    # Fuse and normalize
    final_embedding = CLIPClient.average_embeddings([frame_avg, text_embedding])

    # Serialize to binary if present
    embedding_binary = None
    if final_embedding is not None:
        arr = np.asarray(final_embedding, dtype=np.float32)
        if arr.ndim != 1 or arr.size != 512:
            raise ValueError(f"Invalid embedding shape: {arr.shape}")
        embedding_binary = arr.tobytes()

    # Align result schema to ImageEmbeddingResult (used for image task)
    result_obj = VideoEmbeddingResult(
        embedding=final_embedding,
        embedding_binary=embedding_binary,
        model_name=model_name,
        pretrained=pretrained,
        used_text=bool(merged_text),
        description=description,
        tags=tags,
    )

    return ProcessingResult(
        status="processed",
        result_json=result_obj.model_dump_json(exclude={"embedding_binary"}),
        result_binary=embedding_binary,
        error_message=None,
    )


def processor(items: list[dict]) -> list[ProcessingResult]:
    """
    For each video:
    - Fetch upstream extract_frames and des_tag results
    - Compute per-frame image embeddings and average them
    - Compute text embedding from merged description+tags
    - Fuse (mean) frame_avg and text_avg, normalize
    """
    if len(items) == 0:
        return []

    # Use a shared CLIP client per batch; parse model config from prompt_json
    prompt_jsons = [item.get("prompt_json") for item in items]
    if len(set(prompt_jsons)) != 1:
        raise ValueError("All prompt_jsons must be the same to be batch processed")
    prompt_cfg = (
        json.loads(cast(str, prompt_jsons[0]))
        if prompt_jsons[0]
        else {"model_name": "ViT-B-32", "pretrained": "openai"}
    )
    model_name = prompt_cfg.get("model_name", "ViT-B-32")
    pretrained = prompt_cfg.get("pretrained", "openai")
    clip = CLIPClient(model_name=model_name, pretrained=pretrained)

    results: list[ProcessingResult] = []
    for item in items:
        try:
            results.append(process_one(item, clip, model_name, pretrained))
        except Exception as e:
            tb_str = traceback.format_exc()
            shared_error = f"{e!s}\ntraceback:\n{tb_str[:1000]}"
            results.append(
                ProcessingResult(status="failed", result_json=None, result_binary=None, error_message=shared_error)
            )

    return results


def get_video_embedding_spec() -> TaskSpec:
    return TaskSpec(
        task_key=TaskKey.video_embedding,
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

    spark.sql("DELETE FROM af_delivery_dev.silver_labeling.tasks_status where task_key = 'video.embedding';")
    spark.sql("DELETE FROM af_delivery_dev.silver_labeling.tasks_todo where task_key = 'video.embedding';")
    spark.sql("DELETE FROM af_delivery_dev.silver_labeling.tasks_results where task_key = 'video.embedding';")

    spec = get_video_embedding_spec()
    print("[video_embedding_task] Planning a small batch (limit=1)...")
    batch_ids = build_and_enqueue_todos(spec, limit=1)
    print(f"[video_embedding_task] Planned {len(batch_ids)} batch(es): {batch_ids}")
    assert len(batch_ids) == 1, f"Expected 1 batch, got {len(batch_ids)}"
    for bid in batch_ids:
        print(f"[video_embedding_task] Processing batch_id={bid}...")
        process_batch(bid, spec)
    print("[video_embedding_task] Consolidating...")
    consolidate_task(spec)
    # Optional: small visibility into results count
    results_cnt = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.video_embedding.value)) & (col("media_type") == lit(MediaType.video.value))
        )
        .count()
    )
    print(f"[video_embedding_task] Done. Current total results for task={TaskKey.video_embedding.value}: {results_cnt}")
