import datetime
from enum import Enum

from databricks.sdk.runtime import dbutils, spark
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

dbutils.widgets.text("ENVIRONMENT", "dev")
dbutils.widgets.text("vertical", "labeling")
dbutils.widgets.text("data_domain", "all")
dbutils.widgets.text("file_name_prefix", "")

ENVIRONMENT = dbutils.widgets.get("ENVIRONMENT")
VERTICAL = dbutils.widgets.get("vertical")
SOURCE = dbutils.widgets.get("data_domain")
FILE_NAME_PREFIX = dbutils.widgets.get("file_name_prefix")


CATALOG = f"af_delivery_{ENVIRONMENT}"
NAMESPACE = f"{CATALOG}.landing_{VERTICAL}"
SOURCE_PATH = f"/Volumes/af_delivery_{ENVIRONMENT}/data_collection/{VERTICAL}"

SUPPORTED_MAJOR_TYPES = {"image", "audio", "video"}

LANDING_TABLE_NAME = f"{CATALOG}.landing_{VERTICAL}.media"
PROMPT_PROFILE_TABLE_NAME = f"{CATALOG}.landing_{VERTICAL}.prompts"

SILVER_NAMESPACE = f"{CATALOG}.silver_{VERTICAL}"

# Unified triple-table (shared across tasks and media types)
SILVER_TASKS_TODO_TABLE = f"{SILVER_NAMESPACE}.tasks_todo"
SILVER_TASKS_STATUS_TABLE = f"{SILVER_NAMESPACE}.tasks_status"
SILVER_TASKS_RESULTS_TABLE = f"{SILVER_NAMESPACE}.tasks_results"

GOLD_NAMESPACE = f"{CATALOG}.gold_{VERTICAL}"


def silver_dir_path():
    now = datetime.datetime.now()
    return f"/Volumes/af_delivery_{ENVIRONMENT}/data_collection/{VERTICAL}/silver/{SOURCE}/{now:%Y}/{now:%m}/{now:%d}"


class MediaType(str, Enum):
    image = "image"
    audio = "audio"
    video = "video"


class TaskKey(str, Enum):
    image_des_tag = "image.des_tag"
    image_abuse = "image.abuse"
    image_embedding = "image.embedding"
    video_extract_frames = "video.extract_frames"
    video_des_tag = "video.des_tag"
    video_embedding = "video.embedding"  # depends on video_des_tag and video_extract_frames
    video_frame_abuse = "video.frame_abuse"
    video_frame_abuse_consolidate = "video.frame_abuse_consolidate"
    video_extract_audio = "video.extract_audio"
    video_audio_transcribe = "video.audio_transcribe"
    video_audio_translate = "video.audio_translate"


BRONZE_SCHEMA = StructType(
    [
        StructField("landing_id", StringType(), False, metadata={"comment": "Unique ID for this landing"}),
        StructField("data_domain", StringType(), False, metadata={"comment": "Data domain (required)"}),
        StructField("file_hash", StringType(), False, metadata={"comment": "SHA-256 hash of content (required)"}),
        StructField("file_name", StringType(), True, metadata={"comment": "Name of the ingested file"}),
        StructField("file_size", LongType(), False, metadata={"comment": "Size in bytes"}),
        StructField("mimetype", StringType(), False, metadata={"comment": "Inferred mimetype (required)"}),
        StructField("source_file", StringType(), True, metadata={"comment": "Source file path"}),
        StructField("metadata_file", StringType(), True, metadata={"comment": "Metadata file path"}),
        StructField("original_url", StringType(), True, metadata={"comment": "Google View URL of the file"}),
        StructField("gdrive_path", StringType(), True, metadata={"comment": "Google Drive path of the file"}),
        StructField("ingestion_timestamp", TimestampType(), True, metadata={"comment": "Timestamp of data ingestion"}),
        StructField("modification_time", TimestampType(), False, metadata={"comment": "File last modified timestamp"}),
        # Copied from `trends_amazon_genai_trends.py`
        StructField("created_at", TimestampType(), True, metadata={"comment": "Timestamp of data landing"}),
    ]
)

# manage every prompt by hashing the prompt file.
# the content_json is the prompt file content, or could be the whole prompt profile.
# append only table
PROMPT_TABLE_SCHEMA = StructType(
    [
        StructField("id", StringType(), False, metadata={"comment": "Unique UUID of this abuse category (required)"}),
        StructField("prompt_hash", StringType(), False, metadata={"comment": "python hash of the prompt template"}),
        StructField("content_json", StringType(), True, metadata={"comment": "Prompt profile content jsonified"}),
        StructField("type", StringType(), True, metadata={"comment": "single prompt/or a prompt profile"}),
        StructField(
            "profile_name",
            StringType(),
            False,
            metadata={"comment": "Name of the profile, if this is a prompt profile"},
        ),
        StructField("version", IntegerType(), False, metadata={"comment": "Profile version"}),
        StructField("created_at", TimestampType(), False, metadata={"comment": "When the prompt was created"}),
    ]
)

"""
Silver uses a unified triple-table framework for task processing:

1. TODO Table: Queue of work items to be processed
   - Populated by build_and_enqueue_todos() with candidate (file_hash, prompt_id) pairs
   - Batched using configurable strategies (default: 16 items per batch)
   - Status tracks: pending -> processing -> processed/failed/dlq

2. STATUS Table: Audit log of all processing attempts
   - Append-only event log for observability and debugging
   - Records both start ("processing") and end states for each attempt
   - Enables retry logic and failure analysis

3. RESULTS Table: Final successful outputs
   - Only contains successfully processed items (status="processed")
   - Consolidated from STATUS table via consolidate_task()
   - Used for dependency resolution in downstream tasks

All tables are partitioned by (task_key, media_type) for efficient querying.
"""

# Unified schemas

SILVER_TODO_SCHEMA = StructType(
    [
        # Primary key and batching
        StructField(
            "queue_id",
            LongType(),
            False,
            metadata={"comment": "Deterministic hash of task/media/file/prompt - serves as primary key"},
        ),
        StructField(
            "batch_id", StringType(), False, metadata={"comment": "Run-scoped batch identifier for parallel processing"}
        ),
        # Task identification
        StructField(
            "task_key",
            StringType(),
            False,
            metadata={"comment": "Task type (e.g., 'image.des_tag', 'video.embedding')"},
        ),
        StructField("media_type", StringType(), False, metadata={"comment": "Media type: image|audio|video"}),
        # Work item definition
        StructField("file_hash", StringType(), False, metadata={"comment": "SHA-256 hash linking to landing table"}),
        StructField("source_file", StringType(), True, metadata={"comment": "Original file path for reference"}),
        StructField("prompt_id", StringType(), False, metadata={"comment": "Prompt/profile ID for LLM tasks"}),
        # Processing state
        StructField(
            "status",
            StringType(),
            False,
            metadata={"comment": "Current state: pending|processing|processed|failed|dlq"},
        ),
        StructField(
            "attempts", IntegerType(), False, metadata={"comment": "Number of processing attempts (for retry logic)"}
        ),
        StructField("queued_at", TimestampType(), False, metadata={"comment": "When item was added to queue"}),
        # Lineage and dependencies
        StructField("landing_id", StringType(), False, metadata={"comment": "Links back to original landing record"}),
        StructField(
            "dependency_result_ids",
            ArrayType(LongType()),
            True,
            metadata={"comment": "Array of result_ids this task depends on (for task chaining)"},
        ),
    ]
)

SILVER_STATUS_SCHEMA = StructType(
    [
        # Links to TODO item
        StructField("queue_id", LongType(), False, metadata={"comment": "Links to TODO table queue_id"}),
        StructField("task_key", StringType(), False, metadata={"comment": "Task type for partitioning"}),
        StructField("media_type", StringType(), False, metadata={"comment": "Media type for partitioning"}),
        # Event tracking
        StructField(
            "event_type",
            StringType(),
            False,
            metadata={"comment": "Event type (currently 'attempt' for all processing events)"},
        ),
        StructField(
            "status", StringType(), False, metadata={"comment": "Status at this event: processing|processed|failed|dlq"}
        ),
        StructField("processed_at", TimestampType(), False, metadata={"comment": "When this status event occurred"}),
        StructField("attempts", IntegerType(), False, metadata={"comment": "Attempt number for this event"}),
        # Processing results
        StructField(
            "error_message",
            StringType(),
            True,
            metadata={"comment": "Error details if status=failed, truncated to 1000 chars"},
        ),
        StructField(
            "result_json",
            StringType(),
            True,
            metadata={"comment": "JSON result if status=processed (validated against task output schema)"},
        ),
        # Lineage
        StructField(
            "lineage_id", StringType(), False, metadata={"comment": "Links to original landing_id for traceability"}
        ),
    ]
)

SILVER_RESULTS_SCHEMA = StructType(
    [
        # Primary identification
        StructField(
            "result_id",
            LongType(),
            False,
            metadata={"comment": "Unique result ID (copied from queue_id for successful items)"},
        ),
        StructField(
            "task_key",
            StringType(),
            False,
            metadata={"comment": "Task type for partitioning and dependency resolution"},
        ),
        StructField("media_type", StringType(), False, metadata={"comment": "Media type for partitioning"}),
        # Work item identification (for deduplication)
        StructField("file_hash", StringType(), False, metadata={"comment": "SHA-256 hash of processed file"}),
        StructField("prompt_id", StringType(), False, metadata={"comment": "Prompt/profile ID used for processing"}),
        # Processing outputs
        StructField(
            "result_json", StringType(), False, metadata={"comment": "JSON result validated against task output schema"}
        ),
        StructField(
            "result_binary",
            BinaryType(),
            True,
            metadata={"comment": "Binary result (e.g., embeddings, extracted frames)"},
        ),
        # Metadata
        StructField("created_at", TimestampType(), False, metadata={"comment": "When result was successfully created"}),
        StructField("created_by", StringType(), False, metadata={"comment": "User/principal who processed this item"}),
        # Lineage and dependencies
        StructField("landing_id", StringType(), False, metadata={"comment": "Links to original landing record"}),
        StructField("queue_id", LongType(), False, metadata={"comment": "Links to TODO/STATUS queue_id"}),
        StructField(
            "dependency_result_ids",
            ArrayType(LongType()),
            True,
            metadata={"comment": "Array of upstream result_ids this result depends on"},
        ),
    ]
)

# Ensure unified tables created

# Create unified tables if not exist (partitioned by task_key, media_type)
spark.createDataFrame([], SILVER_TODO_SCHEMA).write.partitionBy("task_key", "media_type").format("delta").mode(
    "ignore"
).saveAsTable(SILVER_TASKS_TODO_TABLE)

spark.createDataFrame([], SILVER_STATUS_SCHEMA).write.partitionBy("task_key", "media_type").format("delta").mode(
    "ignore"
).saveAsTable(SILVER_TASKS_STATUS_TABLE)

spark.createDataFrame([], SILVER_RESULTS_SCHEMA).write.partitionBy("task_key", "media_type").format("delta").mode(
    "ignore"
).saveAsTable(SILVER_TASKS_RESULTS_TABLE)
