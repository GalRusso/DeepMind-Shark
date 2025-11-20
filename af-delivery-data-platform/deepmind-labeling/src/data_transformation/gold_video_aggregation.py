import json

import dlt
from constants import (
    GOLD_NAMESPACE,
    LANDING_TABLE_NAME,
    PROMPT_PROFILE_TABLE_NAME,
    SILVER_TASKS_RESULTS_TABLE,
    MediaType,
    TaskKey,
)
from databricks.sdk.runtime import spark
from deepmerge.merger import Merger
from langchain_core.messages.ai import UsageMetadata
from llm_orchestrator.helpers import calculate_cost, calculate_transcription_cost, categorize_aspect_ratio
from pyspark.sql.functions import (
    array,
    coalesce,
    col,
    collect_list,
    concat_ws,
    first,
    flatten,
    from_json,
    get_json_object,
    lit,
    round,
    transform,
    udf,
)
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# Merge a list of JSON strings (each is dict[str, UsageMetadata]) with deep-merge semantics
def _merge_usage_metadata_list(json_list: list[str] | None) -> str | None:
    if not json_list:
        return None

    def sum_strategy(merger, path, base_value: float, value_to_merge_in: float) -> int | float:
        return base_value + value_to_merge_in

    my_merger = Merger(
        [(list, ["append"]), (dict, ["merge"]), (int, sum_strategy), (float, sum_strategy)],
        ["override"],
        ["override"],
    )

    # Find first non-null as base
    base = None
    for s in json_list:
        if s is not None:
            base = json.loads(s)
            break
    if base is None:
        return None

    for s in json_list:
        if s is None:
            continue
        base = my_merger.merge(base, json.loads(s))
    return json.dumps(base)


merge_usage_udf = udf(_merge_usage_metadata_list, StringType())


@udf(returnType=StringType())
def udf_categorize_aspect_ratio(width: int, height: int) -> str:
    return categorize_aspect_ratio(width, height)


@udf(returnType=DoubleType())
def udf_calculate_cost(usage_metadata: str) -> float | None:
    if not usage_metadata:
        return 0.0
    try:
        usage_metadata = json.loads(usage_metadata)
        assert isinstance(usage_metadata, dict)
        for k, v in usage_metadata.items():
            # parse v from general object to UsageMetadata
            v = UsageMetadata(**v)
            usage_metadata[k] = v
        return calculate_cost(usage_metadata)
    except Exception:
        return None


@udf(returnType=DoubleType())
def udf_calculate_transcription_cost(model: str, transcription_duration_sec: float | None) -> float:
    """model should be whisper-1"""
    if not transcription_duration_sec:
        return 0.0
    return calculate_transcription_cost(model, transcription_duration_sec)


# Parsing schemas for video result_json per task
VIDEO_DES_TAG_JSON_SCHEMA = StructType(
    [
        StructField(
            "desc_result",
            StructType(
                [
                    StructField("description", StringType(), True),
                    StructField("summary", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "tags_result",
            StructType(
                [
                    StructField("tags", ArrayType(StringType()), True),
                ]
            ),
            True,
        ),
    ]
)

VIDEO_ABUSE_PROFILE_JSON_SCHEMA = StructType(
    [
        StructField("abuse_categories", ArrayType(StringType()), True),
        StructField("abuse_categories_conf", ArrayType(DoubleType()), True),
        StructField("abuse_categories_reasoning", ArrayType(StringType()), True),
        StructField("adversary_levels", ArrayType(StringType()), True),
        StructField("adversary_levels_conf", ArrayType(DoubleType()), True),
        StructField("adversary_levels_reasoning", ArrayType(StringType()), True),
    ]
)

VIDEO_FRAMES_JSON_SCHEMA = StructType(
    [
        StructField("frames", ArrayType(StringType()), True),
        StructField("duration", DoubleType(), True),
        StructField("width", IntegerType(), True),
        StructField("height", IntegerType(), True),
    ]
)

TRANSCRIBE_JSON_SCHEMA = StructType(
    [
        StructField(
            "result",
            StructType(
                [
                    StructField("text", StringType(), True),
                    StructField("duration", DoubleType(), True),
                ]
            ),
            True,
        ),
    ]
)

TRANSLATE_JSON_SCHEMA = StructType(
    [
        StructField(
            "translations",
            ArrayType(
                StructType(
                    [
                        StructField("text", StringType(), True),
                        StructField("error", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

VIDEO_EMBEDDING_JSON_SCHEMA = StructType(
    [
        StructField("embedding", ArrayType(DoubleType()), True),
        StructField("embedding_binary", BinaryType(), True),
        StructField("model_name", StringType(), True),
        StructField("pretrained", StringType(), True),
    ]
)


@dlt.view
def resolved_video_results_view():
    base = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(col("media_type") == lit(MediaType.video.value))
        .select(
            "result_id",
            "task_key",
            "media_type",
            "file_hash",
            "prompt_id",
            "result_json",
            "result_binary",
            "created_at",
            "landing_id",
            "queue_id",
            "dependency_result_ids",
        )
    )
    with_group = base.withColumn("group_id", col("file_hash"))
    return with_group.select(
        "result_id",
        "group_id",
        "task_key",
        "media_type",
        "file_hash",
        "prompt_id",
        "result_json",
        "result_binary",
        "created_at",
        "landing_id",
        "queue_id",
    )


@dlt.view
def silver_video_des_tag_view():
    df = dlt.read("resolved_video_results_view").where(
        (col("task_key") == lit(TaskKey.video_des_tag.value)) & (col("media_type") == lit(MediaType.video.value))
    )
    df = df.select(
        "group_id",
        "file_hash",
        "prompt_id",
        from_json(col("result_json"), VIDEO_DES_TAG_JSON_SCHEMA).alias("j"),
        get_json_object(col("result_json"), "$.desc_result.usage_metadata").alias("usage_desc_json"),
        get_json_object(col("result_json"), "$.tags_result.usage_metadata").alias("usage_tags_json"),
    )
    return df.select(
        "group_id",
        "file_hash",
        "prompt_id",
        col("j.desc_result.description").alias("description"),
        col("j.desc_result.summary").alias("summary"),
        col("j.tags_result.tags").alias("tags"),
        col("usage_desc_json").alias("usage_desc_json"),
        col("usage_tags_json").alias("usage_tags_json"),
    )


@dlt.view
def silver_video_abuse_summary_view():
    df = dlt.read("resolved_video_results_view").where(
        (col("task_key") == lit(TaskKey.video_frame_abuse_consolidate.value))
        & (col("media_type") == lit(MediaType.video.value))
    )
    df = df.select(
        "group_id",
        "file_hash",
        "prompt_id",
        from_json(col("result_json"), VIDEO_ABUSE_PROFILE_JSON_SCHEMA).alias("j"),
    )
    return df.select(
        "group_id",
        "file_hash",
        "prompt_id",
        col("j.abuse_categories").alias("abuse_categories"),
        col("j.abuse_categories_conf").alias("abuse_categories_conf"),
        col("j.abuse_categories_reasoning").alias("abuse_categories_reasoning"),
        col("j.adversary_levels").alias("adversary_levels"),
        col("j.adversary_levels_conf").alias("adversary_levels_conf"),
        col("j.adversary_levels_reasoning").alias("adversary_levels_reasoning"),
    )


@dlt.view
def silver_video_embedding_view():
    df = dlt.read("resolved_video_results_view").where(
        (col("task_key") == lit(TaskKey.video_embedding.value)) & (col("media_type") == lit(MediaType.video.value))
    )
    df = df.select(
        "group_id",
        "file_hash",
        "prompt_id",
        from_json(col("result_json"), VIDEO_EMBEDDING_JSON_SCHEMA).alias("j"),
    )
    # NOTE: placeholder for future non-empty embedding schema
    return df.select("group_id", "file_hash", "prompt_id", col("j.embedding").alias("embedding"))


@dlt.view
def silver_video_frames_meta_view():
    df = dlt.read("resolved_video_results_view").where(
        (col("task_key") == lit(TaskKey.video_extract_frames.value)) & (col("media_type") == lit(MediaType.video.value))
    )
    df = df.select(
        "group_id",
        "file_hash",
        "prompt_id",
        from_json(col("result_json"), VIDEO_FRAMES_JSON_SCHEMA).alias("j"),
    )
    return df.select(
        "group_id",
        "file_hash",
        "prompt_id",
        col("j.duration").alias("duration"),
        col("j.width").alias("width"),
        col("j.height").alias("height"),
        udf_categorize_aspect_ratio(col("j.width"), col("j.height")).alias("aspect_ratio"),
    )


@dlt.view
def silver_video_audio_transcribe_view():
    df = dlt.read("resolved_video_results_view").where(
        (col("task_key") == lit(TaskKey.video_audio_transcribe.value))
        & (col("media_type") == lit(MediaType.video.value))
    )
    df = df.select(
        "group_id",
        "file_hash",
        "prompt_id",
        from_json(col("result_json"), TRANSCRIBE_JSON_SCHEMA).alias("j"),
        col("j.result.text").alias("transcript_text"),
        col("j.result.duration").alias("transcription_duration"),
        udf_calculate_transcription_cost(col("prompt_id"), col("transcription_duration")).alias("transcription_cost"),
    )
    return df.select(
        "group_id",
        "file_hash",
        "prompt_id",
        "transcript_text",
        "transcription_cost",
    )


@dlt.view
def silver_video_audio_translate_view():
    df = dlt.read("resolved_video_results_view").where(
        (col("task_key") == lit(TaskKey.video_audio_translate.value))
        & (col("media_type") == lit(MediaType.video.value))
    )
    df = df.select(
        "group_id",
        "file_hash",
        "prompt_id",
        from_json(col("result_json"), TRANSLATE_JSON_SCHEMA).alias("j"),
        get_json_object(col("result_json"), "$.usage_metadata").alias("usage_translate_json"),
    )
    return df.select(
        "group_id",
        "file_hash",
        "prompt_id",
        transform(col("j.translations"), lambda x: x["text"]).alias("translation_texts"),
        col("usage_translate_json").alias("usage_translate_json"),
    )


@dlt.view
def file_meta_view():
    base = spark.table(LANDING_TABLE_NAME).select(
        "file_hash",
        "file_name",
        col("mimetype").alias("file_mime_type"),
        col("source_file").alias("file_path"),
    )
    return base


@dlt.view
def prompt_meta_view():
    df = spark.table(PROMPT_PROFILE_TABLE_NAME).select(
        col("id").alias("prompt_id"), col("content_json").alias("prompts")
    )
    return df


@dlt.create_table(
    name=f"{GOLD_NAMESPACE}.{MediaType.video.value}",
    comment="Unified GOLD table for videos, aggregated from SILVER results",
    table_properties={"quality": "gold"},
)
def gold_videos():
    des = dlt.read("silver_video_des_tag_view").alias("des")
    ab = dlt.read("silver_video_abuse_summary_view").alias("ab")
    # Aggregate abuse-related outputs per group, collecting lists similar to image gold aggregation
    ab = (
        ab.groupBy("group_id")
        .agg(
            first("file_hash", ignorenulls=True).alias("file_hash"),
            first("prompt_id", ignorenulls=True).alias("prompt_id"),
            flatten(collect_list("abuse_categories")).alias("abuse_categories"),
            flatten(collect_list("abuse_categories_conf")).alias("abuse_categories_conf"),
            flatten(collect_list("abuse_categories_reasoning")).alias("abuse_categories_reasoning"),
            flatten(collect_list("adversary_levels")).alias("adversary_levels"),
            flatten(collect_list("adversary_levels_conf")).alias("adversary_levels_conf"),
            flatten(collect_list("adversary_levels_reasoning")).alias("adversary_levels_reasoning"),
        )
        .alias("ab")
    )
    emb = dlt.read("silver_video_embedding_view").alias("emb")
    frm = dlt.read("silver_video_frames_meta_view").alias("frm")
    trc = dlt.read("silver_video_audio_transcribe_view").alias("trc")
    trn = dlt.read("silver_video_audio_translate_view").alias("trn")
    meta = dlt.read("file_meta_view").alias("meta")
    prm = dlt.read("prompt_meta_view").alias("prm")

    gold = des.join(ab, ["group_id"], "full_outer")
    gold = gold.join(emb, ["group_id"], "full_outer")
    gold = gold.join(frm, ["group_id"], "full_outer")
    gold = gold.join(trc, ["group_id"], "full_outer")
    gold = gold.join(trn, ["group_id"], "full_outer")

    gold = gold.withColumn(
        "file_hash_final",
        coalesce(
            col("des.file_hash"),
            col("ab.file_hash"),
            col("emb.file_hash"),
            col("frm.file_hash"),
            col("trc.file_hash"),
            col("trn.file_hash"),
        ),
    ).withColumn(
        "prompt_id_root",
        coalesce(
            col("des.prompt_id"),
            col("ab.prompt_id"),
            col("emb.prompt_id"),
            col("frm.prompt_id"),
        ),
    )

    gold = gold.join(meta, gold["file_hash_final"] == col("meta.file_hash"), "left")
    gold = gold.join(prm, gold["prompt_id_root"] == col("prm.prompt_id"), "left")

    usage_desc = col("des.usage_desc_json")
    usage_tags = col("des.usage_tags_json")
    usage_translate = col("trn.usage_translate_json")
    usage_merged = merge_usage_udf(array(usage_desc, usage_tags, usage_translate))
    usage_cost = udf_calculate_cost(usage_merged)
    total_cost = round(usage_cost + col("trc.transcription_cost"), 6)

    return gold.select(
        col("meta.file_path").alias("file_path"),
        col("meta.file_name").alias("file_name"),
        col("meta.file_mime_type").alias("file_mime_type"),
        col("emb.embedding").alias("embedding"),
        col("des.description").alias("description"),
        col("des.summary").alias("summary"),
        col("des.tags").alias("tags"),
        col("ab.abuse_categories").alias("abuse_categories"),
        col("ab.abuse_categories_conf").alias("abuse_categories_conf"),
        col("ab.abuse_categories_reasoning").alias("abuse_categories_reasoning"),
        col("ab.adversary_levels").alias("adversary_levels"),
        col("ab.adversary_levels_conf").alias("adversary_levels_conf"),
        col("ab.adversary_levels_reasoning").alias("adversary_levels_reasoning"),
        col("frm.duration").alias("duration_seconds"),
        col("frm.aspect_ratio").alias("aspect_ratio"),
        col("trc.transcript_text").alias("transcript_original"),
        concat_ws(" ", col("trn.translation_texts")).alias("transcript_translated"),
        usage_merged.alias("usage_metadata"),
        total_cost.alias("cost"),
        lit(None).cast(DoubleType()).alias("processing_duration"),
        # col("prm.prompts").alias("prompts"),
        col("meta.file_hash").alias("file_hash"),
    )
