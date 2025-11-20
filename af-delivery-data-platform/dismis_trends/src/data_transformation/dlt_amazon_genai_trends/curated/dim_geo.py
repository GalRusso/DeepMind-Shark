import dlt
import pycountry
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType
import sys
sys.path.append("..")
from config import (
    curated_dim_geo_table_name,
    cleansed_latest_table_name
)

def get_country_info(country_name):
    """
    Given a country name, returns a tuple of (alpha_2, numeric, official_name).
    Returns (None, None, None) if the country is not found.
    """
    country = pycountry.countries.get(name=country_name)
    if not country:
        # Try fuzzy search if direct match fails
        try:
            matches = pycountry.countries.search_fuzzy(country_name)
            if matches:
                country = matches[0]
        except Exception as e:
            print(f"An error occurred: {e}")
            country = None

    if country:
        return (
            getattr(country, "alpha_2", None),
            getattr(country, "numeric", None),
            getattr(
                country,
                "official_name",
                getattr(country, "name", getattr(country, "common_name", None)),
            ),
        )

    return (None, None, None)

country_info_schema = StructType([
    StructField("country_code", StringType(), True),
    StructField("numeric", StringType(), True),
    StructField("official_name", StringType(), True)
])

get_country_info_udf = udf(get_country_info, country_info_schema)

@dlt.table(
    name=curated_dim_geo_table_name,
    comment="Dimension table for geographic locations (country) related to Amazon GenAI trends",
    table_properties={
        "quality": "gold"
    }
)
def dim_geo():
    """
    Curated layer: Dimension table for geographic locations.
    Extracts unique country values from the 'geo' array column in the cleansed_latest_table.
    """
    df = dlt.read(cleansed_latest_table_name)

    # Explode the 'geo' array to get individual country values
    dim_geo = (
        df.selectExpr("explode(geo) as country")
            .dropDuplicates()
            .withColumn("country_info", get_country_info_udf("country"))
            .select(
                "country",
                "country_info.country_code",
                "country_info.numeric",
                "country_info.official_name"
            )
    )

    return dim_geo
