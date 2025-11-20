# Bronze: Raw data ingestion from S3 with Autoloader
import mimetypes
import os

import dlt
from constants import BRONZE_SCHEMA, LANDING_TABLE_NAME, SOURCE_PATH
from databricks.sdk.runtime import spark
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, sha2, substring_index, to_timestamp, udf
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

# Bronze layer - Raw data ingestion with Autoloader

dlt.create_streaming_table(
    name=LANDING_TABLE_NAME,
    comment="Raw media files ingested from sources - no optimized",
    schema=BRONZE_SCHEMA,
    table_properties={
        "quality": "bronze",
    },
    partition_cols=["mimetype"],
)


@udf(returnType=StringType())
def get_mimetype(path: str) -> str:
    guessed = mimetypes.guess_type(path)[0]
    return guessed if guessed else "application/octet-stream"


@udf(returnType=StringType())
def get_s3_root_name(path: str) -> str:
    if path.endswith(".metadata.json"):
        path = path.replace(".metadata.json", "")
    return os.path.splitext(path)[0]


# Minimal schema for metadata JSON - only fields we actually need
METADATA_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("version", StringType(), True),
        StructField("webViewLink", StringType(), True),
        StructField("modifiedTime", StringType(), True),
        StructField("sha256Checksum", StringType(), True),
        StructField("size", StringType(), True),
        StructField("headRevisionId", StringType(), True),
        StructField("gdrive_path", StringType(), True),  # Optional field
    ]
)


@dlt.append_flow(
    target=LANDING_TABLE_NAME,
    comment="Raw media files ingested from sources - no optimized",
)
def landing_table_flow():
    """
    Bronze layer: Ingest raw media files from S3 using Autoloader
    """

    metadata_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobfilter", "*.metadata.json")
        .schema(METADATA_SCHEMA)
        .load(SOURCE_PATH)
        .select(
            "id",
            "version",
            col("webViewLink").alias("original_url"),
            "modifiedTime",
            "sha256Checksum",
            "size",
            "headRevisionId",
            "gdrive_path",
        )
        .withColumn("metadata_file", col("_metadata.file_path"))
        .withColumn("s3_root_name", get_s3_root_name(col("metadata_file")))
        .withColumn("modified_time", to_timestamp(col("modifiedTime")))
        .dropDuplicates(["id"])
    )
    file_df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "binaryFile").load(SOURCE_PATH)
    file_df = (
        file_df.withColumn(
            "ingestion_timestamp", col("_metadata.file_modification_time")
        )  # TODO: no information about the ingestion timestamp
        .withColumn("data_domain", lit("ggdrive"))
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("file_name", col("_metadata.file_name"))
        .withColumn("mimetype", get_mimetype(col("source_file")))
        .filter(substring_index(col("mimetype"), "/", 1).isin("video", "audio", "image"))
        # Add other schema fields
        .withColumn("file_size", col("_metadata.file_size"))
        .withColumn("modification_time", col("_metadata.file_modification_time"))
        .withColumn("s3_root_name", get_s3_root_name(col("_metadata.file_path")))
        .drop("content")
        .drop("length")
        .drop("modificationTime")
        .drop("path")
        # identity: (data_domain, file_name, file_size, mimetype)
        .withColumn(
            "file_custom_id",
            sha2(
                concat_ws(
                    "|",
                    col("data_domain"),
                    col("file_name"),
                    col("file_size").cast("string"),
                    col("mimetype"),
                ),
                256,
            ),
        )
    )
    df = file_df.join(metadata_df, on="s3_root_name", how="inner")
    df = (
        df.withColumn("landing_id", col("file_custom_id"))
        .withColumn("created_at", current_timestamp())
        .withColumn("file_hash", col("sha256Checksum"))  # identity provided by the metadata file
        # in-stream deduplication with watermark on processing time
        .withWatermark("created_at", "30 days")
        .dropDuplicates(["file_hash"])  # keep latest by watermark semantics
    ).select(
        [
            "landing_id",
            "data_domain",
            "file_hash",
            "file_name",
            "file_size",
            "mimetype",
            "source_file",
            "metadata_file",
            "original_url",
            "gdrive_path",
            "ingestion_timestamp",
            "modification_time",
            "created_at",
        ]
    )

    return df
