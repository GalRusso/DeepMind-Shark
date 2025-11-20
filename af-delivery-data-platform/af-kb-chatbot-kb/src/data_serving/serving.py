"""From gold to pinecone"""

import argparse
import asyncio
import json
import re
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import nest_asyncio
from constants import GOLD_TRACKING_SCHEMA, ensure_tables_exist, get_table_names
from databricks.sdk.runtime import dbutils, spark
from pinecone import PineconeAsyncio
from pinecone.db_data.types.vector_typed_dict import VectorTypedDict
from pyspark.sql.functions import col
from pyspark.sql.functions import max as spark_max
from tqdm import tqdm

nest_asyncio.apply()

API_KEY = dbutils.secrets.get("datn_af-kb", "pinecone_api_key")


@asynccontextmanager
async def aindex(index_name: str):
    async with PineconeAsyncio(api_key=API_KEY) as client:
        _index_config = await client.describe_index(index_name)
        _host = _index_config["host"]
        async with client.IndexAsyncio(host=_host) as index:
            yield index


async def upsert_to_pinecone(
    index_name: str, namespace: str, ids: list[str], embeddings: list[list[float]], metadatas: list[dict]
):
    """metadata has to have `text` key"""
    vectors = []
    for id, embedding, metadata in zip(ids, embeddings, metadatas, strict=True):
        v = VectorTypedDict(id=id, values=embedding, metadata=metadata)
        vectors.append(v)

    async with aindex(index_name) as index:
        upsert_response = await index.upsert(
            vectors=vectors,
            namespace=namespace,
            batch_size=128,
            show_progress=False,
        )
        return upsert_response.upserted_count


async def get_ids(index_name: str, namespace: str, batch_size: int = 100) -> AsyncGenerator[list[str], None]:
    """pinecone do not support get all the ids once"""
    async with aindex(index_name) as idx:
        async for pine_ids in idx.list(namespace=namespace, limit=batch_size):
            yield pine_ids


async def delete_from_pinecone(index_name: str, namespace: str, ids: list[str]):
    async with aindex(index_name) as idx:
        await idx.delete(ids=ids, namespace=namespace)


async def sync_namespace(
    environment: str, index_name: str, namespace: str, limit: int | None = None, incremental: bool = False
):
    """Sync a single namespace (vertical) to a single Pinecone index.
    Optionally filter by a single Google Drive root folder id.
    Logic: for a Pinecone namespace:
    - Add new vectors to Pinecone
    - Delete stray Pinecone vectors that are missing from Gold (only in full sync)
    - Use gold's chunk_id as pinecone's id
    """
    gold_table_name = get_table_names(environment)["gold"]

    # Get last processed timestamp for incremental mode
    last_processed = None
    if incremental:
        last_processed = get_last_processed_timestamp(environment)
        if last_processed:
            print(f"Incremental mode: Processing chunks created after {last_processed}")

    gold_df = spark.table(gold_table_name).where(col("vertical") == namespace)

    # Apply incremental filter if we have a timestamp
    if last_processed:
        gold_df = gold_df.where(col("created_at") > last_processed)

    gold_df = gold_df.select("chunk_id", "embedding", "metadata", "text", "created_at").orderBy(
        "created_at", "chunk_id"
    )

    if limit:
        gold_df = gold_df.limit(limit)

    total_count = gold_df.count()
    pbar = tqdm(total=total_count, desc="Processing chunks")
    batch_size = 2048
    for offset in range(0, total_count, batch_size):
        batch_df = gold_df.offset(offset).limit(batch_size).collect()
        gold_dict = [row.asDict() for row in batch_df]
        ids = [row["chunk_id"] for row in gold_dict]
        embeddings = [row["embedding"] for row in gold_dict]
        metadatas = []
        for row in gold_dict:
            # Convert metadata from Row object to dictionary
            metadata = row["metadata"]
            if hasattr(metadata, "asDict"):
                metadata = metadata.asDict()
            elif not isinstance(metadata, dict):
                # Handle case where metadata is a Row-like object
                metadata = dict(metadata)

            # NOTE: pinecone do not handle null in metadata
            metadata = {k: v for k, v in metadata.items() if v is not None}
            metadata["text"] = row["text"]  # metadata["text"] is required by pinecone

            # Check metadata size and trim if needed (Pinecone 40KB limit)
            metadata_bytes = len(json.dumps(metadata).encode("utf-8"))
            max_size = 40960  # 40KB
            if metadata_bytes > max_size:
                # Calculate how much to trim
                original_size = metadata_bytes
                text_overhead = metadata_bytes - len(metadata["text"])
                target_text_size = max_size - text_overhead - 100  # 100 byte safety margin
                if target_text_size > 0:
                    metadata["text"] = metadata["text"][:target_text_size]
                    final_size = len(json.dumps(metadata).encode("utf-8"))
                    print(f"Trimmed metadata for chunk_id={row['chunk_id']}: {original_size} -> {final_size} bytes")
                else:
                    print(
                        f"WARNING: chunk_id={row['chunk_id']} metadata too large even without text ({
                            original_size
                        } bytes)"
                    )

            metadatas.append(metadata)
        try:
            updated = await upsert_to_pinecone(index_name, namespace, ids, embeddings, metadatas)
            pbar.update(updated)
        except Exception as e:
            print(f"Error upserting batch (offset {offset}): {e}")
            print(f"Failed chunk_ids: {ids[:5]}... ({len(ids)} total)")
    pbar.close()

    # STEP 2: Delete stray Pinecone vectors that are missing from Gold
    # Only run in full sync mode (not incremental)
    if not incremental:
        print("Running deletion check...")
        # Get all Gold chunk_ids as a DataFrame
        gold_ids_df = spark.table(gold_table_name).where(col("vertical") == namespace).select("chunk_id")
        if limit:
            gold_ids_df = gold_ids_df.orderBy("chunk_id").limit(limit)

        # Delete stray vectors in the single target index
        async for pine_ids in get_ids(index_name, namespace):
            pine_df = spark.createDataFrame([(id,) for id in pine_ids], ["chunk_id"])

            # Find IDs in Pinecone but not in Gold
            missing_df = pine_df.join(gold_ids_df, on="chunk_id", how="left_anti")
            missing_ids = [row.chunk_id for row in missing_df.collect()]

            if missing_ids:
                await delete_from_pinecone(index_name, namespace, missing_ids)
    else:
        print("Skipping deletion check in incremental mode")


def get_namespaces_in_gold(environment: str) -> list[str]:
    gold_table_name = get_table_names(environment)["gold"]
    df = spark.table(gold_table_name).select("vertical").distinct()
    return [row.vertical for row in df.collect()]


def has_new_data(environment: str) -> bool:
    """Check if there's new data in gold table since last processing"""
    table_names = get_table_names(environment)
    tracking_table_name = table_names["gold_tracking"]
    gold_table_name = table_names["gold"]
    tracking_df = spark.table(tracking_table_name)
    tracking_record = tracking_df.first()
    last_processed = None
    if tracking_record:
        last_processed = tracking_record.last_processed_timestamp

    if last_processed is None:
        print("No tracking record found - will process all data")
        return True

    # Get max created_at from gold table
    max_created_df = spark.table(gold_table_name).agg(spark_max("created_at").alias("max_created_at")).first()
    if max_created_df is None:
        print("No data in gold table")
        return False

    max_created_at = max_created_df.max_created_at

    has_new = max_created_at > last_processed
    print(f"Last processed: {last_processed}")
    print(f"Latest data: {max_created_at}")
    print(f"Has new data: {has_new}")

    return has_new


def update_tracking_after_sync(environment: str, stats: dict):
    """Update tracking table after successful sync"""
    tracking_table_name = get_table_names(environment)["gold_tracking"]
    tracking_data = [(datetime.now(UTC), json.dumps(stats))]
    new_tracking_df = spark.createDataFrame(tracking_data, GOLD_TRACKING_SCHEMA)
    new_tracking_df.write.mode("overwrite").format("delta").saveAsTable(tracking_table_name)


def get_last_processed_timestamp(environment: str):
    """Get the last processed timestamp from tracking table"""
    tracking_table_name = get_table_names(environment)["gold_tracking"]
    tracking_df = spark.table(tracking_table_name)
    tracking_record = tracking_df.first()

    if tracking_record is None:
        return None

    return tracking_record.last_processed_timestamp


async def sanity_check(environment: str, index_name: str):
    # number of vectors in gold
    gold_table_name = get_table_names(environment)["gold"]
    df = spark.table(gold_table_name).groupBy("vertical").count().collect()
    # number of vectors in pinecone (single index)
    async with aindex(index_name) as idx:
        stats = await idx.describe_index_stats()
        namespaces_stats = stats.get("namespaces", {})

    # compare gold_count and pinecone_count
    for row in df:
        namespace = row.vertical
        gold_count = row["count"]
        pinecone_count = namespaces_stats.get(namespace, {}).get("vector_count", 0)
        if gold_count != pinecone_count:
            print(f"Mismatch in {namespace}: {gold_count} in gold vs {pinecone_count} in pinecone")

    return stats


async def main(environment: str, index_name: str, limit: int | None = None, incremental: bool = True):
    # Ensure tables exist
    ensure_tables_exist(environment)

    # Check if there's new data to process (full sync always processes)
    if incremental and not has_new_data(environment):
        print("No new data found - skipping sync")
        return

    sync_mode = "incremental" if incremental else "full sync"
    print(f"Running in {sync_mode} mode - starting sync")

    for namespace in tqdm(get_namespaces_in_gold(environment), desc="Syncing namespaces"):
        await sync_namespace(environment, index_name, namespace, limit, incremental)

    stats = await sanity_check(environment, index_name)

    # Update tracking after successful sync
    update_tracking_after_sync(environment, stats.to_dict())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="serving")
    parser.add_argument(
        "--environment",
        required=True,
        choices=["dev", "prod"],
        help="Environment (dev/prod)",
    )
    parser.add_argument("--limit", required=False, help="Limit candidate items (int)")
    parser.add_argument("--index_name", required=True, help="Target Pinecone index name")
    parser.add_argument(
        "--full_sync",
        required=False,
        help="Force full sync instead of incremental (any non-empty value)",
    )
    args = parser.parse_args()
    limit = args.limit
    limit = int(limit) if limit else None
    index_name = args.index_name
    full_sync = bool(args.full_sync)  # Convert to bool - any non-empty string is True
    incremental = not full_sync  # Incremental by default, full sync if flag set
    # Validate index_name format
    if not re.match(r"^[a-z0-9-]+$", index_name):
        raise ValueError(f"Index name '{index_name}' must only contain lowercase letters, numbers, and hyphens")

    asyncio.run(main(args.environment, index_name, limit, incremental))
