import argparse
import asyncio
from datetime import UTC, datetime

import nest_asyncio
import numpy as np
from constants import SILVER_CHUNKING_TABLE, SILVER_VECTORIZING_SCHEMA, SILVER_VECTORIZING_TABLE
from databricks.sdk.runtime import dbutils, spark
from langchain_openai import OpenAIEmbeddings
from pydantic import SecretStr
from pyspark.sql import DataFrame
from pyspark.sql.types import Row
from tqdm import tqdm

nest_asyncio.apply()

EMBED_MODEL = "text-embedding-3-small"
API_KEY = SecretStr(dbutils.secrets.get("datn_af-kb", "openai_api_key"))
EMBEDDINGS = OpenAIEmbeddings(api_key=API_KEY, model=EMBED_MODEL)


async def vectorize_batch(
    *, landing_ids: list[str], chunk_ids: list[str], texts: list[str], batch_size=32
) -> DataFrame:
    assert len(landing_ids) == len(chunk_ids) == len(texts), (
        f"All lists must be of the same length, got: {len(landing_ids)}, {len(chunk_ids)}, {len(texts)}"
    )

    vectors = await EMBEDDINGS.aembed_documents(texts)
    vectors_binary: list[bytes] = []
    for embedding in vectors:
        embedding_np = np.array(embedding)
        vectors_binary.append(embedding_np.tobytes())

    rows = []

    ts_now = datetime.now(UTC)
    for landing_id, chunk_id, text, vec, vec_binary in zip(  # noqa: B007
        landing_ids, chunk_ids, texts, vectors, vectors_binary, strict=True
    ):
        row = Row(
            landing_id=landing_id,
            chunk_id=chunk_id,
            embedding=vec,
            embedding_binary=vec_binary,
            embed_model=EMBED_MODEL,
            created_at=ts_now,
        )
        rows.append(row)

    return spark.createDataFrame(rows, schema=SILVER_VECTORIZING_SCHEMA)


async def main(limit: int | None = None):
    chunk_df = spark.table(SILVER_CHUNKING_TABLE)
    vector_df = spark.table(SILVER_VECTORIZING_TABLE)
    pending_df = chunk_df.join(vector_df, on=["landing_id", "chunk_id"], how="left_anti").select(
        "landing_id", "chunk_id", "text"
    )
    if limit:
        pending_df = pending_df.orderBy(["landing_id", "chunk_id"]).limit(limit)
    pending_list = [row.asDict() for row in pending_df.collect()]
    landing_ids = [row["landing_id"] for row in pending_list]
    chunk_ids = [row["chunk_id"] for row in pending_list]
    texts = [row["text"] for row in pending_list]

    batch_size = 32

    for i in tqdm(range(0, len(texts), batch_size), desc="Vectorizing chunks"):
        batch_landing_ids = landing_ids[i : i + batch_size]
        batch_chunk_ids = chunk_ids[i : i + batch_size]
        batch_texts = texts[i : i + batch_size]
        to_insert_df = await vectorize_batch(
            landing_ids=batch_landing_ids,
            chunk_ids=batch_chunk_ids,
            texts=batch_texts,
        )

        to_insert_df.write.mode("append").saveAsTable(SILVER_VECTORIZING_TABLE)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="silver_runner")
    parser.add_argument("--limit", required=False, help="Limit candidate items (int)")
    args = parser.parse_args()
    limit = args.limit
    limit = int(limit) if limit else None
    asyncio.run(main(limit=limit))
