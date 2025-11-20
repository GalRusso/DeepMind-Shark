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
from pyspark.sql.functions import (
    coalesce,
    col,
    collect_list,
    concat,
    explode_outer,
    first,
    from_json,
    get_json_object,
    lit,
    udf,
    when,
)
from pyspark.sql.functions import (
    min as spark_min,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def _merge_usage_metadata_list(json_list: list[str] | None) -> str | None:
    """each item is jsonlized of `dict[str, UsageMetadata]`"""
    if not json_list:
        return None

    def sum_strategy(merger, path, base_value: float, value_to_merge_in: float) -> int | float:
        """a list strategy to append the last element of nxt only."""
        return base_value + value_to_merge_in

    # Define a custom merger that adds numbers
    my_merger = Merger(
        # A list of strategies to apply based on type
        [
            (list, ["append"]),
            (dict, ["merge"]),
            # Our custom strategy for numbers
            (int, sum_strategy),
            (float, sum_strategy),
        ],
        # Fallback strategies
        ["override"],
        # Strategies for types that don't have defined strategies
        ["override"],
    )
    base = json.loads(json_list[0])
    for s in json_list[1:]:
        base = my_merger.merge(base, json.loads(s))
    return json.dumps(base)


merge_usage_udf = udf(_merge_usage_metadata_list, StringType())

#
# Unified GOLD table (image-level), built from SILVER unified results.
# Requirements from user
# - Remove index_name, remove created_at (not part of GOLD columns now)
# - Prefer a unified GOLD table
# - Columns:
#   file_name, file_mime_type, embedding, description, summary, tags,
#   abuse_categories, abuse_categories_conf, abuse_categories_reasoning,
#   adversary_levels, adversary_levels_conf, adversary_levels_reasoning,
#   cost, aspect_ratio, processing_duration, prompts
#


# ---- Parsing schemas for result_json per task ----

DES_TAG_JSON_SCHEMA = StructType(
    [
        StructField("description", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("tags", ArrayType(StringType()), True),
    ]
)

ABUSE_JSON_SCHEMA = StructType(
    [
        StructField("category_name", StringType(), True),
        StructField("is_category", BooleanType(), True),
        StructField("cat_conf", IntegerType(), True),
        StructField("cat_reason", StringType(), True),
        StructField("adv_level", StringType(), True),
        StructField("adv_conf", IntegerType(), True),
        StructField("adv_reason", StringType(), True),
        # usage_metadata, batch_size ignored for gold projections
    ]
)

EMBEDDING_JSON_SCHEMA = StructType(
    [
        StructField("embedding", ArrayType(DoubleType()), True),
        StructField("model_name", StringType(), True),
        StructField("pretrained", StringType(), True),
        StructField("used_text", StringType(), True),
        StructField("description", StringType(), True),
        StructField("tags", ArrayType(StringType()), True),
    ]
)


@dlt.view
def resolved_results_view():
    # Resolve a stable group_id per result, following dependencies to the upstream image_des_tag when present
    base = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(col("media_type") == lit(MediaType.image.value))
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

    deps = base.withColumn("dep_id", explode_outer(col("dependency_result_ids")))

    upstream_des = (
        spark.table(SILVER_TASKS_RESULTS_TABLE)
        .where(
            (col("task_key") == lit(TaskKey.image_des_tag.value)) & (col("media_type") == lit(MediaType.image.value))
        )
        .select(col("result_id").alias("up_result_id"))
    )

    joined = deps.join(upstream_des, col("dep_id") == col("up_result_id"), "left")

    aggregated = joined.groupBy(
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
    ).agg(spark_min("up_result_id").alias("root_des_tag_id"))

    with_group = aggregated.withColumn(
        "group_id",
        when(col("task_key") == lit(TaskKey.image_des_tag.value), col("result_id")).otherwise(
            coalesce(col("root_des_tag_id"), col("result_id"))
        ),
    )

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
def silver_des_tag_view():
    df = dlt.read("resolved_results_view").where(
        (col("task_key") == lit(TaskKey.image_des_tag.value)) & (col("media_type") == lit(MediaType.image.value))
    )
    df = df.select(
        "group_id",
        "file_hash",
        "prompt_id",
        from_json(col("result_json"), DES_TAG_JSON_SCHEMA).alias("j"),
        get_json_object(col("result_json"), "$.usage_metadata").alias("usage_metadata_json"),
    )
    return df.select(
        "group_id",
        "file_hash",
        "prompt_id",
        col("j.description").alias("description"),
        col("j.summary").alias("summary"),
        col("j.tags").alias("tags"),
        col("usage_metadata_json").alias("usage_metadata_json"),
    )


@dlt.view
def silver_abuse_view():
    df = dlt.read("resolved_results_view").where(
        (col("task_key") == lit(TaskKey.image_abuse.value)) & (col("media_type") == lit(MediaType.image.value))
    )
    df = df.select(
        "group_id",
        "file_hash",
        "prompt_id",
        from_json(col("result_json"), ABUSE_JSON_SCHEMA).alias("j"),
        get_json_object(col("result_json"), "$.usage_metadata").alias("usage_metadata_json"),
    )

    # Keep only positive category matches and aggregate to one row per group_id
    pos = df.select(
        "group_id",
        "file_hash",
        "prompt_id",
        col("usage_metadata_json").alias("usage_metadata_json"),
        col("j.category_name").alias("category_name"),
        col("j.is_category").alias("is_category"),
        col("j.cat_conf").alias("cat_conf"),
        col("j.cat_reason").alias("cat_reason"),
        col("j.adv_level").alias("adv_level"),
        col("j.adv_conf").alias("adv_conf"),
        col("j.adv_reason").alias("adv_reason"),
    ).where(col("is_category") == lit(True))

    aggregated = pos.groupBy("group_id").agg(
        first("file_hash", ignorenulls=True).alias("file_hash"),
        first("prompt_id", ignorenulls=True).alias("prompt_id"),
        collect_list("category_name").alias("abuse_categories"),
        collect_list("cat_conf").alias("abuse_categories_conf"),
        collect_list("cat_reason").alias("abuse_categories_reasoning"),
        collect_list("adv_level").alias("adversary_levels"),
        collect_list("adv_conf").alias("adversary_levels_conf"),
        collect_list("adv_reason").alias("adversary_levels_reasoning"),
        merge_usage_udf(collect_list("usage_metadata_json")).alias("usage_metadata_json"),
    )

    return aggregated


@dlt.view
def silver_embedding_view():
    df = dlt.read("resolved_results_view").where(
        (col("task_key") == lit(TaskKey.image_embedding.value)) & (col("media_type") == lit(MediaType.image.value))
    )
    df = df.select(
        "group_id",
        "file_hash",
        "prompt_id",
        from_json(col("result_json"), EMBEDDING_JSON_SCHEMA).alias("j"),
    )
    return df.select("group_id", "file_hash", "prompt_id", col("j.embedding").alias("embedding"))


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
    # Prefer using prompt table for prompts content; fallback could be TODO table, but not needed now
    df = spark.table(PROMPT_PROFILE_TABLE_NAME).select(
        col("id").alias("prompt_id"), col("content_json").alias("prompts")
    )
    return df


@dlt.create_table(
    name=f"{GOLD_NAMESPACE}.{MediaType.image.value}",
    comment="Unified GOLD table for images, aggregated from SILVER results",
    table_properties={"quality": "gold"},
)
def gold_images():
    des = dlt.read("silver_des_tag_view").alias("des")
    ab = dlt.read("silver_abuse_view").alias("ab")
    emb = dlt.read("silver_embedding_view").alias("emb")
    meta = dlt.read("file_meta_view").alias("meta")
    prm = dlt.read("prompt_meta_view").alias("prm")

    # Join per task on resolved group_id. Outer joins to accept partial availability
    gold = des.join(ab, ["group_id"], "full_outer")
    gold = gold.join(emb, ["group_id"], "full_outer")

    # Coalesce core identifiers from any available source in the group
    gold = gold.withColumn(
        "file_hash_final", coalesce(col("des.file_hash"), col("ab.file_hash"), col("emb.file_hash"))
    ).withColumn("prompt_id_root", coalesce(col("des.prompt_id"), col("ab.prompt_id"), col("emb.prompt_id")))

    gold = gold.join(meta, gold["file_hash_final"] == col("meta.file_hash"), "left")
    gold = gold.join(prm, gold["prompt_id_root"] == col("prm.prompt_id"), "left")

    # Build unified usage_metadata JSON string per row by merging available per-task metadata
    usage_des = col("des.usage_metadata_json")
    usage_ab = col("ab.usage_metadata_json")
    usage_both = (
        when(
            usage_des.isNotNull() & usage_ab.isNotNull(),
            concat(lit('{"image.des_tag":'), usage_des, lit(',"image.abuse":'), usage_ab, lit("}")),
        )
        .when(usage_des.isNotNull() & usage_ab.isNull(), concat(lit('{"image.des_tag":'), usage_des, lit("}")))
        .when(usage_des.isNull() & usage_ab.isNotNull(), concat(lit('{"image.abuse":'), usage_ab, lit("}")))
        .otherwise(lit(None).cast(StringType()))
    )

    # Fill currently unavailable fields with nulls as per contract
    gold = gold.select(
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
        usage_both.alias("usage_metadata"),
        lit(None).cast(StringType()).alias("aspect_ratio"),
        lit(None).cast(DoubleType()).alias("processing_duration"),
    )

    return gold
