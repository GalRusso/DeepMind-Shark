"""Parsing and Chunking with docling"""

import argparse
import hashlib
from datetime import UTC, datetime

import tiktoken
from constants import (
    LANDING_TABLE_NAME,
    SILVER_CHUNKING_DLQ_SCHEMA,
    SILVER_CHUNKING_DLQ_TABLE,
    SILVER_CHUNKING_SCHEMA,
    SILVER_CHUNKING_TABLE,
)
from databricks.sdk.runtime import spark
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling_core.transforms.chunker.hybrid_chunker import HybridChunker
from docling_core.transforms.chunker.tokenizer.openai import OpenAITokenizer
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, Row
from tqdm import tqdm

NUM_TASKS = 5

# pinecone limit 40kB (40960 bytes) for each metadata.
# 1 token ~= 4 characters
tokenizer = OpenAITokenizer(tokenizer=tiktoken.get_encoding("cl100k_base"), max_tokens=4 * 1024)
chunker = HybridChunker(tokenizer=tokenizer, merge_peers=True)
pipeline_opts = PdfPipelineOptions(do_ocr=False)
converter = DocumentConverter(format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_opts)})


def parse_chunk(local_path: str, landing_id: str) -> DataFrame:
    doc = converter.convert(local_path).document
    rows = []
    for c in chunker.chunk(doc):
        chunk_hash = hashlib.sha256(c.model_dump_json().encode()).hexdigest()
        row = Row(
            landing_id=landing_id,
            chunk_id=chunk_hash,
            text=c.text,
            metadata=c.meta.export_json_dict(),
            created_at=datetime.now(UTC),
        )
        rows.append(row)

    return spark.createDataFrame(rows, schema=SILVER_CHUNKING_SCHEMA)


@udf(returnType=IntegerType())
def hash_landing_id(landing_id: str) -> int:
    """Hash landing_id to distribute tasks evenly"""
    return int(hashlib.sha256(landing_id.encode()).hexdigest(), 16) % NUM_TASKS


def get_pending_list(remaining: int) -> list[dict]:
    landing_df = spark.table(LANDING_TABLE_NAME)
    chunk_df = spark.table(SILVER_CHUNKING_TABLE)
    dlq_df = spark.table(SILVER_CHUNKING_DLQ_TABLE)

    # Exclude both successfully processed chunks and DLQ failures
    pending_df = landing_df.join(chunk_df, on="landing_id", how="left_anti").join(
        dlq_df, on="landing_id", how="left_anti"
    )

    pending_df = (
        pending_df.withColumn("hash_landing_id", hash_landing_id(col("landing_id")))
        .where(col("hash_landing_id") == remaining)
        .select("landing_id", "source_file")
    )
    pending_list = [row.asDict() for row in pending_df.collect()]
    return pending_list


parser = argparse.ArgumentParser()
parser.add_argument("--remaining", type=int, required=True)
args = parser.parse_args()
remaining = args.remaining
pending_list = get_pending_list(remaining)

for row in tqdm(pending_list, desc="Parsing and chunking documents"):
    try:
        chunk_df = parse_chunk(row["source_file"], row["landing_id"])
        chunk_df.write.mode("append").saveAsTable(SILVER_CHUNKING_TABLE)
    except Exception as e:
        print(f"Error parsing chunk: {e}")
        # Write failure to DLQ
        dlq_row = spark.createDataFrame(
            [
                {
                    "landing_id": row["landing_id"],
                    "source_file": row["source_file"],
                    "error_message": str(e),
                    "failed_at": datetime.now(UTC),
                }
            ],
            schema=SILVER_CHUNKING_DLQ_SCHEMA,
        )
        dlq_row.write.mode("append").saveAsTable(SILVER_CHUNKING_DLQ_TABLE)
