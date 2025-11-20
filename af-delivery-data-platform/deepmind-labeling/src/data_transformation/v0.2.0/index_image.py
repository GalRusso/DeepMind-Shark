"""Reimplement the `process_image` function in victor_v0.2.0_indexing.py"""

import time
from datetime import UTC, datetime

from deepmerge.merger import Merger
from langchain_core.messages.ai import UsageMetadata
from llm_orchestrator.helpers import calculate_cost
from llm_orchestrator.image_abuse_profiler import ImageAbuseProfiler, ImageAbuseProfilerResult
from llm_orchestrator.image_des_tag import ImageDescriptionTagProcessor, ImageDescriptionTagResult
from llm_orchestrator.image_embedding import CLIPClient, ImageEmbeddingResult
from llm_orchestrator.llm_helpers import init_chat_model
from prompt_profiles_config import P_IMAGE_DESCRIPTION, P_PROFILE_MVP, ImageAbuseCategory


def generate_image_description_tags(local_file_path) -> ImageDescriptionTagResult:
    prompt = P_IMAGE_DESCRIPTION

    llms = []
    for model in prompt.models:
        llms.append(init_chat_model(model))

    processor = ImageDescriptionTagProcessor(
        image_desription_prompt=prompt.description_prompt_template.format(),
        image_tag_prompt=prompt.tag_prompt_template.format(),
        llms=llms,
    )

    batch_results = processor.batch([local_file_path])
    return batch_results[0]


def generate_image_embedding(local_file_path, description, tags) -> ImageEmbeddingResult:
    model_name = "ViT-B-32"
    pretrained = "openai"

    clip = CLIPClient(model_name=model_name, pretrained=pretrained)
    batch_results = clip.batch([local_file_path], [description], [tags])
    return batch_results[0]


def generate_image_abuse_category(
    local_file_path, description, tags, prompt: ImageAbuseCategory
) -> ImageAbuseProfilerResult:
    llms = []
    for model in prompt.models:
        llms.append(init_chat_model(model))
    profiler = ImageAbuseProfiler(abuse_category=prompt, llms=llms)
    results = profiler.batch([local_file_path], [description], [tags])
    return results[0]


def merge_usage_metadata(usage_metadata_list: list[dict[str, UsageMetadata]]) -> dict[str, UsageMetadata]:
    def sum_strategy(merger, path, base_value: float, value_to_merge_in: float) -> int | float:
        """a list strategy to append the last element of nxt only."""
        return base_value + value_to_merge_in

    # Define a custom merger that adds numbers
    my_merger = Merger(
        # A list of strategies to apply based on type
        [
            (list, ["append"]),
            (dict, ["merge"]),
            # Our custom strategy for numbers
            (int, sum_strategy),
            (float, sum_strategy),
        ],
        # Fallback strategies
        ["override"],
        # Strategies for types that don't have defined strategies
        ["override"],
    )
    base = usage_metadata_list[0]
    for s in usage_metadata_list[1:]:
        base = my_merger.merge(base, s)
    return base


def process_image(local_file_path):
    start = time.time()
    description_tags = generate_image_description_tags(local_file_path)
    embedding = generate_image_embedding(local_file_path, description_tags.description, description_tags.tags)
    abuse_categories = []
    for image_abuse_prompt in P_PROFILE_MVP.image_abuse_categories:
        abuse_category = generate_image_abuse_category(
            local_file_path, description_tags.description, description_tags.tags, image_abuse_prompt
        )
        abuse_categories.append(abuse_category)

    processing_duration = time.time() - start
    # calculate cost
    usage_metadata = merge_usage_metadata(
        [description_tags.usage_metadata, *[ab.usage_metadata for ab in abuse_categories]]
    )
    cost = calculate_cost(usage_metadata)

    abuse_categories = []
    abuse_categories_conf = []
    abuse_categories_reasonings = []
    adversary_levels = []
    adversary_levels_conf = []
    adversary_levels_reasonings = []
    for ab in abuse_categories:
        if not ab.is_category:
            continue
        abuse_categories.append(ab.category_name)
        abuse_categories_conf.append(ab.cat_conf)
        abuse_categories_reasonings.append(ab.cat_reason)
        adversary_levels.append(ab.adv_level)
        adversary_levels_conf.append(ab.adv_conf)
        adversary_levels_reasonings.append(ab.adv_reason)

    # Other fields

    return {
        "fingerprint": None,  # won't have this
        "created_at": datetime.now(UTC),
        "file_name": local_file_path.split("/")[-1],
        "file_size": 0,  # placeholder
        "external_url": "none",  # placeholder
        "file_type": "image/jpeg",  # placeholder
        "embedding": embedding.embedding,
        "description": description_tags.description,
        "summary": description_tags.summary,
        "tags": description_tags.tags,
        "abuse_categories": abuse_categories,
        "abuse_categories_conf": abuse_categories_conf,
        "abuse_categories_reasonings": abuse_categories_reasonings,
        "adversary_levels": adversary_levels,
        "adversary_levels_conf": adversary_levels_conf,
        "adversary_levels_reasonings": adversary_levels_reasonings,
        "processing_cost_usd": cost,
        "aspect_ratio": "none",  # placeholder
        "length": 0.0,  # placeholder
        "transcript_original": [],
        "transcript_translated": [],
        "index_name": "none",  # placeholder
        "prompts": [],
        "processing_duration": processing_duration,
        "reviewed_abuse_categories": [],
        "reviewed_adversary_levels": [],
        "prompt_profile": P_PROFILE_MVP.name,
    }


if __name__ == "__main__":
    from pprint import pprint

    ret = process_image(
        "../../tests/fixtures/BigBuckBunny_320x180_extracted_frame_223d483cc6c2483aa13f2274069a96c0.png"
    )
    pprint(ret)
