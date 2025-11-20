import dlt
import sys
sys.path.append("..")
from config import (
    curated_dim_trends_table_name,
    cleansed_latest_table_name
)

@dlt.table(
    name=curated_dim_trends_table_name,
    comment="Dimension table for Amazon GenAI trends",
    table_properties={
        "quality": "gold"
    }
)
def dim_trends():
    """
    Curated layer: Dimension table for Amazon GenAI trends.
    Extracts unique trend attributes from the cleansed_latest_table.
    """

    # Extract trend dimension data
    dim_trends = (
        dlt.read(cleansed_latest_table_name)
        .select(
            "trend_id",
            "item_id",
            "item_name",
            "geo",
            "language",
            "trend_type",
            "trend_sub_category",
            "key_modality",
            "description",
            "additional_context",
            "sample_source",
            "screenshots",
            "platform_presence",
        )
    )

    return dim_trends
