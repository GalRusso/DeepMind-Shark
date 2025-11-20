from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import dlt
from pyspark.sql.functions import col, row_number, when, regexp_extract, lit, md5
from pyspark.sql.window import Window
import sys
sys.path.append('..')
from config import dim_mainstream_media_table, trends_latest_table_name

# Schema for mainstream media dimension
msm_dim_schema = StructType([
    StructField("msm_key", IntegerType(), False, metadata={"comment": "Mainstream media surrogate key"}),
    StructField("msm_domain", StringType(), True, metadata={"comment": "MSM domain extracted from URL (e.g., cnn.com)"}),
    StructField("msm_name", StringType(), True, metadata={"comment": "MSM outlet name (e.g., CNN)"}),
    StructField("msm_category", StringType(), True, metadata={"comment": "MSM category (TV, Online, Print, etc.)"}),
    StructField("is_major_news", BooleanType(), True, metadata={"comment": "Flag for major news outlets"}),
    StructField("is_international", BooleanType(), True, metadata={"comment": "Flag for international outlets"}),
    StructField("msm_hash", StringType(), True, metadata={"comment": "Hash for MSM natural key"})
])

@dlt.table(
    name=dim_mainstream_media_table,
    comment="Mainstream media dimension table for MSM analysis",
    schema=msm_dim_schema,
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def dim_mainstream_media():
    """
    Create mainstream media dimension table by extracting domains from msm_link.
    Supports MSM source analysis and outlet categorization.
    """
    # Read trends data and extract domains from msm_link
    trends_df = dlt.read(trends_latest_table_name)
    
    # Extract domains from msm_link URLs
    msm_domains = (
        trends_df
        .select(
            regexp_extract(col("msm_link"), r"https?://(?:www\.)?([^/]+)", 1).alias("msm_domain")
        )
        .filter(col("msm_domain").isNotNull() & (col("msm_domain") != ""))
        .distinct()
    )
    
    # Add MSM attributes
    msm_with_attributes = msm_domains.select(
        col("msm_domain"),
        
        # Extract outlet name from domain
        when(col("msm_domain").rlike("(?i)cnn"), "CNN")
        .when(col("msm_domain").rlike("(?i)bbc"), "BBC")
        .when(col("msm_domain").rlike("(?i)fox"), "Fox News")
        .when(col("msm_domain").rlike("(?i)nbc"), "NBC")
        .when(col("msm_domain").rlike("(?i)abc"), "ABC")
        .when(col("msm_domain").rlike("(?i)cbs"), "CBS")
        .when(col("msm_domain").rlike("(?i)reuters"), "Reuters")
        .when(col("msm_domain").rlike("(?i)bloomberg"), "Bloomberg")
        .when(col("msm_domain").rlike("(?i)nytimes"), "New York Times")
        .when(col("msm_domain").rlike("(?i)wsj"), "Wall Street Journal")
        .when(col("msm_domain").rlike("(?i)washingtonpost"), "Washington Post")
        .when(col("msm_domain").rlike("(?i)guardian"), "The Guardian")
        .when(col("msm_domain").rlike("(?i)independent"), "The Independent")
        .when(col("msm_domain").rlike("(?i)telegraph"), "The Telegraph")
        .when(col("msm_domain").rlike("(?i)dailymail"), "Daily Mail")
        .when(col("msm_domain").rlike("(?i)times"), "The Times")
        .when(col("msm_domain").rlike("(?i)ft"), "Financial Times")
        .when(col("msm_domain").rlike("(?i)ap"), "Associated Press")
        .when(col("msm_domain").rlike("(?i)afp"), "AFP")
        .when(col("msm_domain").rlike("(?i)dpa"), "DPA")
        .when(col("msm_domain").rlike("(?i)spiegel"), "Der Spiegel")
        .when(col("msm_domain").rlike("(?i)faz"), "Frankfurter Allgemeine")
        .when(col("msm_domain").rlike("(?i)lemonde"), "Le Monde")
        .when(col("msm_domain").rlike("(?i)lefigaro"), "Le Figaro")
        .when(col("msm_domain").rlike("(?i)elpais"), "El País")
        .when(col("msm_domain").rlike("(?i)corriere"), "Corriere della Sera")
        .when(col("msm_domain").rlike("(?i)repubblica"), "La Repubblica")
        .when(col("msm_domain").rlike("(?i)asahi"), "Asahi Shimbun")
        .when(col("msm_domain").rlike("(?i)yomiuri"), "Yomiuri Shimbun")
        .when(col("msm_domain").rlike("(?i)mainichi"), "Mainichi Shimbun")
        .when(col("msm_domain").rlike("(?i)china"), "China Daily")
        .when(col("msm_domain").rlike("(?i)xinhua"), "Xinhua")
        .when(col("msm_domain").rlike("(?i)people"), "People's Daily")
        .when(col("msm_domain").rlike("(?i)timesofindia"), "Times of India")
        .when(col("msm_domain").rlike("(?i)hindustan"), "Hindustan Times")
        .when(col("msm_domain").rlike("(?i)thehindu"), "The Hindu")
        .when(col("msm_domain").rlike("(?i)indianexpress"), "Indian Express")
        .when(col("msm_domain").rlike("(?i)straitstimes"), "Straits Times")
        .when(col("msm_domain").rlike("(?i)scmp"), "South China Morning Post")
        .otherwise(col("msm_domain")).alias("msm_name"),
        
        # Categorize MSM outlets
        when(col("msm_domain").rlike("(?i)(cnn|fox|nbc|abc|cbs|bbc)"), "TV News")
        .when(col("msm_domain").rlike("(?i)(nytimes|wsj|washingtonpost|guardian|independent|telegraph|times|ft)"), "Print/Online")
        .when(col("msm_domain").rlike("(?i)(reuters|bloomberg|ap|afp|dpa)"), "News Agency")
        .when(col("msm_domain").rlike("(?i)(spiegel|faz|lemonde|lefigaro|elpais|corriere|repubblica)"), "European Media")
        .when(col("msm_domain").rlike("(?i)(asahi|yomiuri|mainichi|china|xinhua|people|timesofindia|hindustan|thehindu|indianexpress|straitstimes|scmp)"), "Asian Media")
        .otherwise("Other").alias("msm_category"),
        
        # Flag for major news outlets
        when(col("msm_domain").rlike("(?i)(cnn|bbc|fox|nbc|abc|cbs|reuters|bloomberg|nytimes|wsj|washingtonpost|guardian|ap|afp)"), True)
        .otherwise(False).alias("is_major_news"),
        
        # Flag for international outlets
        when(col("msm_domain").rlike("(?i)(bbc|reuters|bloomberg|guardian|independent|telegraph|times|ft|ap|afp|dpa|spiegel|faz|lemonde|lefigaro|elpais|corriere|repubblica|asahi|yomiuri|mainichi|china|xinhua|people|timesofindia|hindustan|thehindu|indianexpress|straitstimes|scmp)"), True)
        .otherwise(False).alias("is_international"),
        
        md5(col("msm_domain")).alias("msm_hash")
    )
    
    # Add surrogate key
    window_spec = Window.orderBy("msm_domain")
    final_df = msm_with_attributes.select(
        row_number().over(window_spec).alias("msm_key"),
        col("msm_domain"),
        col("msm_name"),
        col("msm_category"),
        col("is_major_news"),
        col("is_international"),
        col("msm_hash")
    )
    
    return final_df
