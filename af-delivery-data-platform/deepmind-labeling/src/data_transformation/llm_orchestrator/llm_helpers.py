import base64
import itertools
import mimetypes
import os
from collections.abc import Callable, Sequence
from typing import Any, TypeVar

from google.genai import types
from langchain.chat_models import init_chat_model as langchain_init_chat_model
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage
from langchain_core.rate_limiters import InMemoryRateLimiter
from pydantic import BaseModel

GOOGLE_GEMINI_SAFETY_SETTINGS = [
    types.SafetySetting(
        category=types.HarmCategory.HARM_CATEGORY_HARASSMENT, threshold=types.HarmBlockThreshold.BLOCK_NONE
    ),
    types.SafetySetting(
        category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH, threshold=types.HarmBlockThreshold.BLOCK_NONE
    ),
    types.SafetySetting(
        category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT, threshold=types.HarmBlockThreshold.BLOCK_NONE
    ),
    types.SafetySetting(
        category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, threshold=types.HarmBlockThreshold.BLOCK_NONE
    ),
    types.SafetySetting(
        category=types.HarmCategory.HARM_CATEGORY_CIVIC_INTEGRITY, threshold=types.HarmBlockThreshold.BLOCK_NONE
    ),
]

StateT = TypeVar("StateT", bound="BaseModel | dict")
OutputT = TypeVar("OutputT", bound=BaseModel)


def structured_llm_fallback(
    *,
    state: StateT,
    llms: list[BaseChatModel],
    schema: type[OutputT],
    build_messages_list: Sequence[Callable[[StateT], HumanMessage]],
    ok: Callable[[OutputT | Any], bool] | None = None,
) -> tuple[OutputT | None, int | None, str | None, Exception | None]:
    """
    Try each combination of message building strategy and LLM provider sequentially
    with structured output until one succeeds.

    The cartesian product of `build_messages_list` and `providers` is tried in order.

    Returns (result, message_builder_index, provider_name, last_exception).
    """
    last_exc: Exception | None = None
    builder_indexes = list(range(len(build_messages_list)))
    fallbacks = itertools.product(builder_indexes, llms)
    for builder_index, llm in fallbacks:
        builder_function = build_messages_list[builder_index]
        if llm is None:
            continue
        try:
            human_messages = builder_function(state)
            result = llm.with_structured_output(schema).invoke([human_messages])
            if not isinstance(result, schema):
                result = schema.model_validate(result)
            if ok is None:
                return result, builder_index, llm.name, None
            elif ok(result):
                return result, builder_index, llm.name, None
            # Mark as failure if response did not meet acceptance criteria
            last_exc = ValueError(f"{llm.name} returned invalid result")
        except Exception as exc:
            last_exc = exc
    return None, None, None, last_exc


async def astructured_llm_fallback(
    *,
    state: StateT,
    llms: list[BaseChatModel],
    schema: type[OutputT],
    build_messages_list: Sequence[Callable[[StateT], HumanMessage]],
    ok: Callable[[OutputT | Any], bool] | None = None,
) -> tuple[OutputT | None, int | None, str | None, Exception | None]:
    """
    Try each combination of message building strategy and LLM provider sequentially
    with structured output until one succeeds.

    The cartesian product of `build_messages_list` and `providers` is tried in order.

    Returns (result, message_builder_index, provider_name, last_exception).
    """
    last_exc: Exception | None = None
    builder_indexes = list(range(len(build_messages_list)))
    fallbacks = itertools.product(builder_indexes, llms)
    for builder_index, llm in fallbacks:
        builder_function = build_messages_list[builder_index]
        if llm is None:
            continue
        try:
            human_messages = builder_function(state)
            result = await llm.with_structured_output(schema).ainvoke([human_messages])
            if not isinstance(result, schema):
                result = schema.model_validate(result)
            if ok is None:
                return result, builder_index, llm.name, None
            elif ok(result):
                return result, builder_index, llm.name, None
            # Mark as failure if response did not meet acceptance criteria
            last_exc = ValueError(f"{llm.name} returned invalid result")
        except Exception as exc:
            last_exc = exc
    return None, None, None, last_exc


def init_chat_model(model: str) -> BaseChatModel:
    """exten langchain_core.language_models.chat_models.init_chat_model with:
    - correct api key
    - rate limiter"""
    from databricks.sdk.runtime import dbutils
    from langchain.chat_models.base import _parse_model as parse_model

    model_name, provider = parse_model(model, None)
    rate_limiter = InMemoryRateLimiter(requests_per_second=100, max_bucket_size=100)  # no real usage for now

    api_key = None
    config = None
    match provider:
        case "openai":
            api_key = dbutils.secrets.get(scope="deepmind-labeling", key="openai_api_key")
        case "google_genai":
            api_key = dbutils.secrets.get(scope="deepmind-labeling", key="gemini_api_key")
            # FIXME: is this really works?
            config = types.GenerateContentConfig(safety_settings=GOOGLE_GEMINI_SAFETY_SETTINGS)

    kwargs = {}
    if api_key:
        kwargs["api_key"] = api_key
    if config:
        kwargs["model_kwargs"] = {"generation_config": config}

    return langchain_init_chat_model(model, rate_limiter=rate_limiter, **kwargs)


def construct_gemini_video_msg(
    prompt_text: str, file_path: str | None = None, uploaded_file_info: dict | None = None
) -> HumanMessage:
    """construct a gemini video message from either file path or uploaded file info"""
    parts: list[dict] = [{"type": "text", "text": prompt_text}]

    if uploaded_file_info:
        parts.append(
            {
                "type": "media",
                "mime_type": uploaded_file_info.get("mime_type"),
                "file_uri": uploaded_file_info.get("uri"),
            }
        )

    if file_path:
        mime_type, _ = mimetypes.guess_type(file_path)
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File does not exist: {file_path}")

        mime_type, _ = mimetypes.guess_type(file_path)
        mime_type = mime_type or "application/octet-stream"

        file_size = os.path.getsize(file_path)
        if file_size > 20 * 1024 * 1024:
            raise ValueError(f"File exceeds 20MB size limit, cannot use inline: {file_path}, size: {file_size}")

        with open(file_path, "rb") as video_file:
            encoded_video = base64.b64encode(video_file.read()).decode("utf-8")

        parts.append(
            {
                "type": "media",
                "data": encoded_video,  # Use base64 string directly
                "mime_type": mime_type,
            },
        )

    return HumanMessage(content=parts)


def construct_image_msg(prompt_text: str, file_path: str) -> HumanMessage:
    mime_type, _ = mimetypes.guess_type(file_path)
    if not (mime_type and mime_type.startswith("image/")):
        raise ValueError(f"Unsupported file type: {file_path}")
    with open(file_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")
    return HumanMessage(
        content=[
            {"type": "text", "text": prompt_text},
            {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{b64}"}},
        ]
    )


if __name__ == "__main__":
    init_chat_model("gpt-4o-mini")
    init_chat_model("openai:gpt-4o")
    init_chat_model("google_genai:gemini-2.5-flash")
