import json
import traceback

from constants import (
    LANDING_TABLE_NAME,
    SILVER_TASKS_RESULTS_TABLE,
    SILVER_TASKS_TODO_TABLE,
    MediaType,
    TaskKey,
)
from databricks.sdk.runtime import spark
from llm_orchestrator.image_embedding import CLIPClient
from pyspark.sql.functions import (
    array,
    col,
    lit,
    struct,
    to_json,
    xxhash64,
)
from task_framework import ProcessingResult, TaskSpec


def build_image_embedding_candidates_df(limit: int | None = None):
    """
    Build candidates for image embedding.

    - Images come from LANDING_TABLE_NAME
    - Require successful upstream image.des_tag (description/tags)
    - Use a deterministic prompt_id/prompt_json to identify the embedding configuration
    - Compute dependency_result_ids pointing to upstream des_tag result_id
    """
    bronze_df = spark.table(LANDING_TABLE_NAME).select(
        "file_hash", "mimetype", "file_size", "landing_id", "source_file"
    )
    bronze_df = bronze_df.filter("split(mimetype, '/')[0] IN ('image')")
    if limit is not None:
        bronze_df = bronze_df.orderBy("landing_id").limit(limit)

    # Deterministic prompt/config for CLIP
    # Keep small JSON to stabilize queue_id hashing
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
            (col("task_key") == lit(TaskKey.image_embedding.value)) & (col("media_type") == lit(MediaType.image.value))
        )
        .select("file_hash", "prompt_id")
    )
    todo_df = (
        spark.table(SILVER_TASKS_TODO_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.image_embedding.value)) & (col("media_type") == lit(MediaType.image.value))
        )
        .select("file_hash", "prompt_id")
    )
    candidates_df = candidates_df.join(silver_df, ["file_hash", "prompt_id"], "left_anti")
    candidates_df = candidates_df.join(todo_df, ["file_hash", "prompt_id"], "left_anti")

    # Require upstream successful description/tag with its prompt_id, to compute dependency_result_ids
    upstream_success_df = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.image_des_tag.value)) & (col("media_type") == lit(MediaType.image.value))
        )
        .select("file_hash", col("prompt_id").alias("prompt_des_tag_id"))
    )

    to_insert_df = candidates_df.join(upstream_success_df, ["file_hash"], "inner")
    to_insert_df = to_insert_df.withColumn(
        "upstream_result_id",
        xxhash64(
            lit(TaskKey.image_des_tag.value),
            lit(MediaType.image.value),
            col("file_hash"),
            col("prompt_des_tag_id"),
        ),
    )
    to_insert_df = to_insert_df.drop("file_size", "mimetype").withColumn(
        "dependency_result_ids", array(col("upstream_result_id"))
    )

    return to_insert_df


def image_embedding_processor(items: list[dict]) -> list[ProcessingResult]:
    if len(items) == 0:
        return []

    # Use a shared CLIP client per batch; parse model config from prompt_json
    # Ensure a single prompt_json per batch for determinism
    prompt_jsons = [item.get("prompt_json") for item in items]
    if len(set(prompt_jsons)) != 1:
        raise ValueError("All prompt_jsons must be the same to be batch processed")
    prompt_cfg = json.loads(prompt_jsons[0]) if prompt_jsons[0] else {"model_name": "ViT-B-32", "pretrained": "openai"}
    model_name = prompt_cfg.get("model_name", "ViT-B-32")
    pretrained = prompt_cfg.get("pretrained", "openai")

    clip = CLIPClient(model_name=model_name, pretrained=pretrained)

    # Build batch aligned inputs for CLIP using framework-enriched upstream_parsed
    file_paths: list[str] = []
    descriptions: list[str | None] = []
    tag_lists: list[list[str] | None] = []
    for item in items:
        source_file = item.get("source_file")
        description: str | None = None
        tags: list[str] | None = None
        for upstream in item.get("upstream_parsed", []) or []:
            # Accept either ImageDescriptionTagResult or similar schema with description/tags
            u_desc = getattr(upstream, "description", None)
            u_tags = getattr(upstream, "tags", None)
            if u_desc is not None or (u_tags and len(u_tags) > 0):
                description = u_desc
                tags = u_tags
                break
        file_paths.append(str(source_file))
        descriptions.append(description)
        tag_lists.append(tags)

    results: list[ProcessingResult] = []
    try:
        batch_results = clip.batch(file_paths, descriptions, tag_lists)
        for r in batch_results:
            results.append(
                ProcessingResult(
                    status="processed",
                    result_json=r.model_dump_json(exclude={"embedding_binary"}),
                    result_binary=r.embedding_binary,
                    error_message=None,
                )
            )
    except Exception as e:
        tb_str = traceback.format_exc()
        shared_error = f"{e!s}\ntraceback:\n{tb_str[:1000]}"
        for _ in range(len(items)):
            results.append(
                ProcessingResult(status="failed", result_json=None, result_binary=None, error_message=shared_error)
            )

    return results


def get_image_embedding_spec() -> TaskSpec:
    return TaskSpec(
        task_key=TaskKey.image_embedding,
        media_type=MediaType.image,
        prompt_table_type=None,
        processor=image_embedding_processor,
        max_attempts=3,
        build_candidates_df=build_image_embedding_candidates_df,
    )
