"""Reimplement the `process_image` function in victor_v0.2.0_indexing.py"""

import asyncio
import tempfile
import time
from datetime import UTC, datetime

import nest_asyncio
import numpy as np
from databricks.sdk.runtime import dbutils
from deepmerge.merger import Merger
from langchain_core.messages.ai import UsageMetadata
from llm_orchestrator.audio_transcribe import AudioTranscribe
from llm_orchestrator.audio_translate import AudioTranslate, TranslateResult
from llm_orchestrator.helpers import calculate_cost, calculate_transcription_cost, merge_description_and_tags
from llm_orchestrator.image_abuse_profiler import ImageAbuseProfiler, ImageAbuseProfilerResult
from llm_orchestrator.image_embedding import CLIPClient
from llm_orchestrator.llm_helpers import init_chat_model
from llm_orchestrator.video_des_tag import VideoDescriptionTagProcessor, VideoDescriptionTagResult
from llm_orchestrator.video_frame_abuse_consolidation import consolidate_abuse_categories
from llm_orchestrator.video_helpers import extract_audio_from_video, extract_frames, get_video_width_height
from models import MaybeAudioTranscribeResult, VideoEmbeddingResult
from prompt_profiles_config import (
    P_PROFILE_MVP,
    P_TEXT_TRANSLATION,
    P_VIDEO_DESCRIPTION,
    ImageAbuseCategory,
    PTImageDesTag,
    VideoAbuseCategory,
)

nest_asyncio.apply()


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


def generate_video_description_tags(local_file_path: str, source_frames: list[str]) -> VideoDescriptionTagResult:
    prompt = P_VIDEO_DESCRIPTION
    llms = []
    for model in prompt.models:
        llms.append(init_chat_model(model))

    frame_llms = []
    for model in prompt.frame_models:
        frame_llms.append(init_chat_model(model))

    processor = VideoDescriptionTagProcessor(
        prompt=prompt,
        llms=llms,
        fallback_llms=frame_llms,
        gemini_api_key=dbutils.secrets.get(scope="Gal Russo Keys", key="Gemini Api key CT"),
    )

    batch_results = asyncio.run(processor.abatch([local_file_path], [source_frames]))
    return batch_results[0]


def generate_video_embedding(frame_paths, description, tags) -> VideoEmbeddingResult:
    model_name = "ViT-B-32"
    pretrained = "openai"

    clip = CLIPClient(model_name=model_name, pretrained=pretrained)

    # Compute frame-only embeddings and average
    frame_embeddings: list[list[float] | None] = []
    for frame_path in frame_paths:
        frame_embeddings.append(clip.embed_image(frame_path))
    frame_avg = CLIPClient.average_embeddings(frame_embeddings)

    # Compute text-only embedding
    text_embedding: list[float] | None = None
    merged_text = merge_description_and_tags(description, tags)
    text_embedding = clip.embed_text(merged_text)

    # Fuse and normalize
    final_embedding = CLIPClient.average_embeddings([frame_avg, text_embedding])

    # Serialize to binary if present
    embedding_binary = None
    if final_embedding is not None:
        arr = np.asarray(final_embedding, dtype=np.float32)
        if arr.ndim != 1 or arr.size != 512:
            raise ValueError(f"Invalid embedding shape: {arr.shape}")
        embedding_binary = arr.tobytes()

    result_obj = VideoEmbeddingResult(
        embedding=final_embedding,
        embedding_binary=embedding_binary,
        model_name=model_name,
        pretrained=pretrained,
        used_text=bool(merged_text),
        description=description,
        tags=tags,
    )
    return result_obj


def generate_frame_abuse_category(
    frame_file_paths: list[str], description, tags, prompt: VideoAbuseCategory
) -> list[ImageAbuseProfilerResult]:
    llms = []
    for model in prompt.models:
        llms.append(init_chat_model(model))

    profiler = ImageAbuseProfiler(
        abuse_category=ImageAbuseCategory(
            version=prompt.version,
            models=prompt.models,
            category_name=prompt.category_name,
            abuse_category_prompt_template=prompt.abuse_category_prompt_template,
            adversary_levels_prompt_template=prompt.adversary_levels_prompt_template,
            category_definition=prompt.category_definition,
            adversary_levels=prompt.adversary_levels,
            pt_image_desc_tag=PTImageDesTag(
                version=prompt.pt_video_desc_tag.version,
                models=prompt.pt_video_desc_tag.models,
                description_prompt_template=prompt.pt_video_desc_tag.pt_video_desc,
                tag_prompt_template=prompt.pt_video_desc_tag.pt_video_tags,
            ),
        ),
        llms=llms,
    )
    frame_batch_results = profiler.batch(
        file_paths=frame_file_paths,
        descriptions=[description] * len(frame_file_paths),
        tagss=[tags] * len(frame_file_paths),
    )
    return frame_batch_results


def process_audio(local_file_path, tmp_dir) -> tuple[MaybeAudioTranscribeResult, TranslateResult]:
    audio_path = extract_audio_from_video(local_file_path, outdir=tmp_dir)
    if not audio_path:
        return MaybeAudioTranscribeResult(result=None, reason="no audio for this file"), TranslateResult(
            translations=[], usage_metadata={}
        )

    transcriber = AudioTranscribe(
        model="whisper-1",
        openai_key=dbutils.secrets.get(scope="deepmind-labeling", key="openai_api_key"),
    )
    _batch_results = asyncio.run(transcriber.abatch([audio_path]))

    transcription_result = MaybeAudioTranscribeResult(result=_batch_results[0])
    # translate
    if not transcription_result.result:
        return transcription_result, TranslateResult(translations=[], usage_metadata={})

    prompt = P_TEXT_TRANSLATION

    llms = []
    for model in prompt.models:
        llms.append(init_chat_model(model))

    translator = AudioTranslate(prompt=prompt, llms=llms)

    translate_results = asyncio.run(translator.abatch([[t.text for t in transcription_result.result.segments]]))

    return transcription_result, translate_results[0]


def process_video(local_file_path, tmp_dir):
    start = time.time()
    usage_metadata = []

    # audio extraction
    transcription_result, translate_results = process_audio(local_file_path, tmp_dir)
    usage_metadata.append(translate_results.usage_metadata)

    frame_paths, duration = extract_frames(local_file_path, output_dir=tmp_dir)
    assert frame_paths is not None, f"frame_paths is {frame_paths}"
    width, height = get_video_width_height(local_file_path)

    description_tags = generate_video_description_tags(local_file_path, frame_paths)
    description = description_tags.desc_result.description
    tags = description_tags.tags_result.tags
    usage_metadata.append(description_tags.desc_result.usage_metadata)
    usage_metadata.append(description_tags.tags_result.usage_metadata)

    embedding = generate_video_embedding(frame_paths, description, tags)

    frame_abuse_categories, frame_abuse_categories_conf, frame_abuse_categories_reasoning = [], [], []
    frame_adversary_levels, frame_adversary_levels_conf, frame_adversary_levels_reasoning = [], [], []
    for image_abuse_prompt in P_PROFILE_MVP.video_abuse_categories:
        frame_abuse_category = generate_frame_abuse_category(frame_paths, description, tags, image_abuse_prompt)
        usage_metadata.extend([ab.usage_metadata for ab in frame_abuse_category])

        for frame in frame_abuse_category:
            if not frame.is_category:
                continue
            frame_abuse_categories.append(frame.category_name)
            frame_abuse_categories_conf.append(frame.cat_conf)
            frame_abuse_categories_reasoning.append(frame.cat_reason)
            frame_adversary_levels.append(frame.adv_level)
            frame_adversary_levels_conf.append(frame.adv_conf)
            frame_adversary_levels_reasoning.append(frame.adv_reason)

    video_profile_result = consolidate_abuse_categories(
        abuse_categories=frame_abuse_categories,
        abuse_categories_conf=frame_abuse_categories_conf,
        abuse_categories_reasoning=frame_abuse_categories_reasoning,
        adversary_levels=frame_adversary_levels,
        adversary_levels_conf=frame_adversary_levels_conf,
        adversary_levels_reasoning=frame_adversary_levels_reasoning,
    )

    processing_duration = time.time() - start
    # construct result

    # calculate cost
    usage_metadata = merge_usage_metadata(usage_metadata)
    cost = calculate_cost(usage_metadata) + calculate_transcription_cost("whisper-1", duration)

    # Other fields

    return {
        "fingerprint": None,  # won't have this
        "created_at": datetime.now(UTC),
        "file_name": local_file_path.split("/")[-1],
        "file_size": 0,  # placeholder
        "external_url": "none",  # placeholder
        "file_type": "video/mp4",  # placeholder
        "embedding": embedding.embedding,
        "description": description,
        "summary": description_tags.desc_result.summary,
        "tags": tags,
        "abuse_categories": video_profile_result.abuse_categories,
        "abuse_categories_conf": video_profile_result.abuse_categories_conf,
        "abuse_categories_reasonings": video_profile_result.abuse_categories_reasoning,
        "adversary_levels": video_profile_result.adversary_levels,
        "adversary_levels_conf": video_profile_result.adversary_levels_conf,
        "adversary_levels_reasonings": video_profile_result.adversary_levels_reasoning,
        "processing_cost_usd": cost,
        "aspect_ratio": f"{width}:{height}",  # placeholder
        "length": duration,
        "transcript_original": transcription_result.result.text,
        "transcript_translated": [t.text for t in translate_results.translations],
        "index_name": "none",  # placeholder
        "prompts": [],
        "processing_duration": processing_duration,
        "reviewed_abuse_categories": [],
        "reviewed_adversary_levels": [],
        "prompt_profile": P_PROFILE_MVP.name,
    }


if __name__ == "__main__":
    from pprint import pprint

    with tempfile.TemporaryDirectory() as tmp_dir:
        ret = process_video("../../tests/fixtures/BigBuckBunny_320x180_trimmed.mp4", tmp_dir)
        pprint(ret)
