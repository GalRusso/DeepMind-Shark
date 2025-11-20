# %%
from constants import PROMPT_PROFILE_TABLE_NAME, PROMPT_TABLE_SCHEMA
from databricks.sdk.runtime import spark
from delta import DeltaTable
from prompt_profiles_config import (
    P_IMAGE_DESCRIPTION,
    P_PROFILE_MVP,
    P_TEXT_TRANSLATION,
    P_VIDEO_DESCRIPTION,
    PromptTableType,
)
from pyspark.sql.functions import expr


def create_table_from_fixtures():
    rows = []
    rows.append(
        {
            "type": PromptTableType.image_description_tag.value,
            "content_json": P_IMAGE_DESCRIPTION.model_dump_json(),
            "prompt_hash": P_IMAGE_DESCRIPTION.hash(),
            "version": P_IMAGE_DESCRIPTION.version,
            "profile_name": None,
        }
    )
    rows.append(
        {
            "type": PromptTableType.video_description_tag.value,
            "content_json": P_VIDEO_DESCRIPTION.model_dump_json(),
            "prompt_hash": P_VIDEO_DESCRIPTION.hash(),
            "version": P_VIDEO_DESCRIPTION.version,
            "profile_name": None,
        }
    )
    rows.append(
        {
            "type": PromptTableType.text_translation.value,
            "content_json": P_TEXT_TRANSLATION.model_dump_json(),
            "prompt_hash": P_TEXT_TRANSLATION.hash(),
            "version": P_TEXT_TRANSLATION.version,
            "profile_name": None,
        }
    )
    for abuse_category in P_PROFILE_MVP.image_abuse_categories:
        rows.append(
            {
                "type": PromptTableType.image_abuse_category.value,
                "content_json": abuse_category.model_dump_json(),
                "prompt_hash": abuse_category.hash(),
                "version": abuse_category.version,
                "profile_name": P_PROFILE_MVP.name,
            }
        )

    for abuse_category in P_PROFILE_MVP.video_abuse_categories:
        rows.append(
            {
                "type": PromptTableType.video_abuse_category.value,
                "content_json": abuse_category.model_dump_json(),
                "prompt_hash": abuse_category.hash(),
                "version": abuse_category.version,
                "profile_name": P_PROFILE_MVP.name,
            }
        )
    return spark.createDataFrame(rows)


fixtures_df = create_table_from_fixtures()

# %%

# Create table if not exists
spark.createDataFrame([], schema=PROMPT_TABLE_SCHEMA).write.format("delta").mode("ignore").saveAsTable(
    PROMPT_PROFILE_TABLE_NAME
)
delta_table = DeltaTable.forName(spark, PROMPT_PROFILE_TABLE_NAME)

insert_mapping = {col: expr(f"source.{col}") for col in fixtures_df.columns}
insert_mapping["created_at"] = expr("current_timestamp()")
insert_mapping["id"] = expr("uuid()")

delta_table.alias("target").merge(
    fixtures_df.alias("source"), "target.prompt_hash = source.prompt_hash"
).whenNotMatchedInsert(values=insert_mapping).execute()

print("Updated/Inserted prompts with changes.")
# %%
