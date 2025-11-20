import dlt
import sys
sys.path.append("..")
from config import (
    curated_dim_language_table_name,
    cleansed_latest_table_name
)

@dlt.table(
    name=curated_dim_language_table_name,
    comment="Dimension table for languages related to Amazon GenAI trends",
    table_properties={
        "quality": "gold"
    }
)
def dim_language():
    """
    Curated layer: Dimension table for languages.
    Extracts unique language values from the 'language' array column in the cleansed_latest_table.
    """
    df = dlt.read(cleansed_latest_table_name)

    # Explode the 'language' array to get individual language values
    dim_language = (
        df.selectExpr("explode(language) as language")
            .dropDuplicates()
            .select("language")
    )

    return dim_language
