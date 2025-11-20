# Bronze: Raw data ingestion from S3 with Autoloader
# Refactored to use independent source tables for better debugging and incremental processing
import mimetypes
import os

import dlt
from constants import BRONZE_SCHEMA, LANDING_TABLE_NAME, MONDAY_SOURCE_PATH, SOURCE_PATH
from databricks.sdk.runtime import spark
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    from_json,
    lit,
    lower,
    sha2,
    split,
    to_timestamp,
    trim,
    udf,
    when,
)
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

# ============================================================================
# UDF Definitions
# ============================================================================

@udf(returnType=StringType())
def get_mimetype(path: str) -> str:
    guessed = mimetypes.guess_type(path)[0]
    return guessed if guessed else "application/octet-stream"


@udf(returnType=StringType())
def get_s3_root_name(path: str) -> str:
    if path.endswith(".metadata.json"):
        path = path.replace(".metadata.json", "")
    return os.path.splitext(path)[0]


# ============================================================================
# Schema Definitions
# ============================================================================

# Schema for Monday JSON files from discovery job
MONDAY_ITEM_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("team", StringType(), True),
        StructField("client", StringType(), True),
        StructField("owner", StringType(), True),
        StructField("bug_number", StringType(), True),
        StructField("type", StringType(), True),
        StructField("delivery_date", StringType(), True),
        StructField("rag_kb_url", StringType(), True),
        StructField("activemind_status", StringType(), True),
        StructField("final_report_file", StringType(), True),
        StructField("final_report_url", StringType(), True),
        StructField("original_item", ArrayType(StringType()), True),
        StructField("match", StringType(), True),
    ]
)

# Schema for old monday_metadata JSON string (backwards compatibility)
OLD_MONDAY_METADATA_SCHEMA = StructType(
    [
        StructField("board_id", StringType(), True),
        StructField("id", StringType(), True),  # This is the item_id
    ]
)

# Minimal schema for metadata JSON - only fields we actually need
# Note: All fields are optional (True) to handle files with missing fields
METADATA_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("version", StringType(), True),
        StructField("mimeType", StringType(), True),
        StructField("webViewLink", StringType(), True),
        StructField("modifiedTime", StringType(), True),
        StructField("sha256Checksum", StringType(), True),
        StructField("size", StringType(), True),
        StructField("headRevisionId", StringType(), True),
        StructField("gdrive_path", StringType(), True),
        # New format (top-level)
        StructField("board_id", StringType(), True),
        StructField("group_id", StringType(), True),
        StructField("item_id", StringType(), True),
        # Old format (nested in monday_metadata JSON string)
        StructField("monday_metadata", StringType(), True),
        # Export format for Google Workspace files (Docs, Sheets, Slides)
        StructField("export_format_used", StringType(), True),
    ]
)


# ============================================================================
# Source Table 1: Monday Items
# ============================================================================

@dlt.table(
    name="monday_items",
    comment="Monday.com items with metadata - materialized for independent processing",
    table_properties={"quality": "bronze"},
)
def monday_items():
    """
    Ingest Monday JSON files from discovery job.
    Each file contains metadata about reports from Monday.com boards.
    No deduplication here - handled in landing table.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("pathGlobfilter", "*.json")
        .schema(MONDAY_ITEM_SCHEMA)
        .load(MONDAY_SOURCE_PATH)
        .select(
            col("id").alias("item_id"),
            col("team").alias("team"),
            # Split owner by comma, trim whitespace, and convert to lowercase
            when(col("owner").isNull() | (trim(col("owner")) == ""), lit(None))
            .otherwise(split(lower(trim(col("owner"))), r"\s*,\s*"))
            .alias("owner"),
            col("client").alias("client"),
            col("bug_number").alias("bug_number"),
            col("name").alias("report_name"),
            col("type").alias("type"),
            col("delivery_date").alias("delivery_date"),
            col("final_report_url").alias("original_url"),
            col("_metadata.file_modification_time").alias("monday_mod_time"),
        )
    )


# ============================================================================
# Source Table 2: File Metadata
# ============================================================================

@dlt.table(
    name="file_metadata",
    comment="Google Drive metadata JSON files - materialized for independent processing",
    table_properties={"quality": "bronze"},
)
def file_metadata():
    """
    Ingest Google Drive metadata JSON files.
    Handles both new format (top-level fields) and old format (nested monday_metadata).
    No deduplication here - handled in landing table.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobfilter", "*.metadata.json")
        .option("multiLine", "true")
        .schema(METADATA_SCHEMA)
        .load(SOURCE_PATH)
        .withColumn("monday_parsed", from_json(col("monday_metadata"), OLD_MONDAY_METADATA_SCHEMA))
        .select(
            col("id").alias("gdrive_id"),
            col("version").cast("string").alias("version"),
            col("modifiedTime").cast("string").alias("modifiedTime"),
            col("sha256Checksum").cast("string").alias("sha256Checksum"),
            col("size").cast("string").alias("size"),
            col("headRevisionId").cast("string").alias("headRevisionId"),
            col("gdrive_path").cast("string").alias("gdrive_path"),
            # Use export_format_used for Google Workspace files, otherwise use mimeType
            coalesce(col("export_format_used"), col("mimeType")).alias("mime_type"),
            # Coalesce: try new format first, fall back to old format
            coalesce(col("board_id"), col("monday_parsed.board_id")).alias("board_id"),
            coalesce(col("item_id"), col("monday_parsed.id")).alias("item_id"),
            coalesce(col("group_id"), lit("group_mkwzb81w")).alias("group_id"),  # Default for old format
            col("_metadata.file_path").alias("metadata_file"),
            col("_metadata.file_modification_time").alias("metadata_mod_time"),
        )
        .withColumn("s3_root_name", get_s3_root_name(col("metadata_file")))
        .withColumn("modified_time", to_timestamp(col("modifiedTime")))
    )


# ============================================================================
# Source Table 3: Files
# ============================================================================

@dlt.table(
    name="files",
    comment="Binary files with basic metadata - materialized for independent processing",
    table_properties={"quality": "bronze"},
)
def files():
    """
    Ingest binary files from S3.
    Extracts file metadata and computes hash for deduplication.
    No deduplication here - handled in landing table.
    
    Note: Excel files larger than 1MB are filtered out and tracked in ignored_files table.
    """
    file_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobfilter", "*.{doc,docx,pdf,txt,csv,xls,xlsx,ppt,pptx,html}")
        .load(SOURCE_PATH)
        .withColumn("file_name", col("_metadata.file_name"))
        .withColumn("file_size", col("_metadata.file_size"))
        .withColumn("ingestion_ts", col("_metadata.file_modification_time"))
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("guessed_mime_type", get_mimetype(col("source_file")))
        .withColumn("s3_root_name", get_s3_root_name(col("_metadata.file_path")))
        .withColumn("modification_time", col("_metadata.file_modification_time"))
        .withColumn("file_hash", sha2(col("content"), 256))
        .drop("content", "length", "modificationTime", "path")
    )
    # Exclude Excel files larger than 1MB
    file_df = file_df.filter(~(col("guessed_mime_type").contains("sheet") & (col("file_size") > 1024 * 1024 * 1)))
    
    return file_df


# ============================================================================
# Ignored Files Table - Tracks files that are filtered out
# ============================================================================

@dlt.table(
    name="ignored_files",
    comment="Files that are ignored during processing (e.g., Excel files > 1MB, JSON files) - for Monday feedback",
    table_properties={"quality": "bronze"},
)
def ignored_files():
    """
    Track files that are intentionally ignored during processing.
    Currently tracks:
    - Excel files larger than 1MB
    - JSON files (not supported for text extraction)
    
    This table is used by the serving layer to report back to Monday.com
    that these files exist but cannot be processed.
    """
    # Load all potentially ignored file types
    file_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobfilter", "*.{xls,xlsx,json}")
        .load(SOURCE_PATH)
        .withColumn("file_name", col("_metadata.file_name"))
        .withColumn("file_size", col("_metadata.file_size"))
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("guessed_mime_type", get_mimetype(col("source_file")))
        .withColumn("s3_root_name", get_s3_root_name(col("_metadata.file_path")))
        .withColumn("ingestion_ts", col("_metadata.file_modification_time"))
        .drop("content", "length", "modificationTime", "path")
        # Exclude metadata.json files
        .filter(~col("file_name").endswith(".metadata.json"))
    )
    
    # Filter and assign appropriate ignore reasons
    ignored_df = file_df.filter(
        # Excel files >= 1MB OR JSON files
        (col("guessed_mime_type").contains("sheet") & (col("file_size") >= 1024 * 1024 * 1))
        | col("guessed_mime_type").contains("json")
    ).withColumn(
        "ignore_reason",
        when(
            col("guessed_mime_type").contains("json"),
            lit("JSON file type not supported")
        ).otherwise(
            lit("Excel file too large (>1MB)")
        )
    ).select(
        "file_name",
        "file_size",
        "source_file",
        "s3_root_name",
        "guessed_mime_type",
        "ingestion_ts",
        "ignore_reason",
    )
    
    return ignored_df


# ============================================================================
# Final Landing Table - Joins all sources
# ============================================================================

dlt.create_streaming_table(
    name=LANDING_TABLE_NAME,
    comment="Raw media files ingested from sources - no optimized",
    schema=BRONZE_SCHEMA,
    table_properties={
        "quality": "bronze",
    },
    partition_cols=["mime_type"],
)


@dlt.append_flow(
    target=LANDING_TABLE_NAME,
    comment="Raw media files ingested from sources - joined from independent source tables",
)
def landing_table_flow():
    """
    Join the three independent source tables using stream-to-static joins.
    This approach allows each source to process independently while maintaining
    inner join semantics for the final output.
    
    Deduplication is handled here with watermark + dropDuplicates to keep
    unique records by file_hash within the watermark window.
    """
    # Read from materialized source tables using dlt.read_stream()
    files_df = dlt.read_stream("files")
    metadata_df = dlt.read_stream("file_metadata")
    monday_df = dlt.read_stream("monday_items")
    
    # Join 1: Files with their metadata (on s3_root_name)
    df = files_df.join(metadata_df, on="s3_root_name", how="inner")
    
    # Join 2: Result with Monday items (on item_id)
    df = df.join(
        monday_df,
        df.item_id == monday_df.item_id,
        how="inner",
    ).drop(monday_df.item_id)  # Drop duplicate item_id from monday_df
    
    # Final transformations and deduplication
    df = (
        df.withColumn("landing_id", col("file_hash"))
        .withColumn("landing_ts", current_timestamp())
        # Streaming deduplication: watermark + dropDuplicates
        # Keeps unique records by file_hash within 30-day watermark window
        .withWatermark("ingestion_ts", "30 days")
        .dropDuplicates(["file_hash"])
    ).select(
        "landing_id",
        "file_name",
        "file_hash",
        "file_size",
        col("guessed_mime_type").alias("mime_type"),
        "ingestion_ts",
        "source_file",
        "metadata_file",
        "original_url",
        "gdrive_path",
        "modified_time",
        "team",
        "client",
        "type",
        "delivery_date",
        "owner",
        "bug_number",
        "report_name",
        "board_id",
        "group_id",
        "item_id",
        "landing_ts",
    )
    
    return df
