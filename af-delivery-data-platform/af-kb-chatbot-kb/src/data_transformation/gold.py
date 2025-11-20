import dlt
from constants import GOLD_SCHEMA, GOLD_TABLE, LANDING_TABLE_NAME, SILVER_CHUNKING_TABLE, SILVER_VECTORIZING_TABLE
from pyspark.sql import Row
from pyspark.sql.functions import coalesce, col, first, greatest, lit, max, to_date, udf, unix_timestamp
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType


@udf(
    returnType=StructType(
        [
            StructField("file_path", StringType(), True),
            StructField("vertical", StringType(), True),
            StructField("last_modified", LongType(), True),
            StructField("client", StringType(), True),
            StructField("delivery_date", LongType(), True),
            StructField("subtype", StringType(), True),
            StructField("owner", ArrayType(StringType()), True),
            StructField("bug_number", StringType(), True),
            StructField("report_name", StringType(), True),
        ]
    )
)
def create_metadata(
    original_url: str,
    vertical: str,
    last_modified_unix: int,
    client: str,
    delivery_date_unix: int,
    subtype: str,
    owner: list,
    bug_number: str,
    report_name: str,
) -> Row:
    """This will be inserted to pinecone as metadata.
    https://docs.pinecone.io/guides/index-data/indexing-overview#metadata-format
    """
    return Row(
        file_path=original_url,  # backward compatibility
        vertical=vertical,
        last_modified=last_modified_unix,  # Keep as integer
        client=client,
        delivery_date=delivery_date_unix,  # Same value as last_modified
        subtype=subtype,
        owner=owner,
        bug_number=bug_number,
        report_name=report_name,
    )


@dlt.table(
    name=GOLD_TABLE,
    comment="Unified GOLD table for chunks, aggregated from SILVER results",
    table_properties={"quality": "gold"},
    schema=GOLD_SCHEMA,
    partition_cols=["vertical"],
)
def gold():
    landing_df = dlt.read(LANDING_TABLE_NAME).drop("created_at")  # Keep landing_ts
    chunk_df = dlt.read(SILVER_CHUNKING_TABLE).drop("metadata").drop("created_at")
    vector_df = dlt.read(SILVER_VECTORIZING_TABLE)  # Keep created_at for deduplication

    # Join all tables and add computed columns before deduplication
    joined_df = (
        landing_df.join(chunk_df, on="landing_id", how="inner")
        .join(vector_df, on=["landing_id", "chunk_id"], how="inner")
        .withColumn("vertical", coalesce(col("team"), lit("unknown")))
        .withColumn(
            "metadata",
            create_metadata(
                col("original_url"),
                col("vertical"),
                unix_timestamp(to_date(col("delivery_date"), "yyyy-MM-dd")),
                col("client"),
                unix_timestamp(to_date(col("delivery_date"), "yyyy-MM-dd")),  # Parse delivery_date string to Unix timestamp
                col("type"),  # Will be stored as subtype in metadata
                col("owner"),
                col("bug_number"),
                col("report_name"),
            ),
        )
    )

    # Deduplicate using GROUP BY - keep latest created_at for each landing_id + chunk_id combination
    gold_df = (
        joined_df.groupBy("landing_id", "chunk_id")
        .agg(
            max(greatest(col("landing_ts"), col("created_at"))).alias(
                "created_at"
            ),  # Max of landing_ts and silver created_at
            first("original_url").alias("original_url"),
            first("vertical").alias("vertical"),
            first("text").alias("text"),
            first("metadata").alias("metadata"),
            first("embedding").alias("embedding"),
            first("embedding_binary").alias("embedding_binary"),
            first("board_id").alias("board_id"),
            first("group_id").alias("group_id"),
            first("item_id").alias("item_id"),
        )
        .select(
            "landing_id",
            "chunk_id",
            "original_url",
            "vertical",
            "text",
            "metadata",
            "embedding",
            "embedding_binary",
            "created_at",
            "board_id",
            "group_id",
            "item_id",
        )
    )

    return gold_df
