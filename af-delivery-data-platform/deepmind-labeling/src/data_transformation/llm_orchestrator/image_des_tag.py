import base64
import mimetypes
import re
from typing import Annotated

from langchain_core.callbacks import get_usage_metadata_callback
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage
from langchain_core.messages.ai import UsageMetadata
from llm_orchestrator.llm_helpers import structured_llm_fallback
from pydantic import BaseModel, BeforeValidator, Field, constr


class ImageDescription(BaseModel):
    description: str | None = Field(
        default=None,
        description="A single paragraph of up to 200 words describing the image content, written in natural English without referencing the original format (e.g., image).",
    )
    summary: str | None = Field(default=None, description="A one-line summary in English in 12 words or fewer.")


def clean_tag(tag: str) -> str:
    if not isinstance(tag, str):
        return tag
    tag = tag.replace("\\", "").replace('"', "")
    tag = tag.strip().strip("/")
    tag = tag.replace(".", "")
    tag = tag.lower()
    if tag.count('"') > 2:
        return ""
    tag = re.sub(r"[^a-zA-Z0-9\s_-]", "", tag)
    tag = tag.replace("-", "_")
    tag = re.sub(r"[\s]+", "_", tag)
    return tag if tag else ""


TagType = Annotated[
    constr(strip_whitespace=True, to_lower=True, pattern=r"^[a-z0-9_]+$"),
    BeforeValidator(clean_tag),
]


class ImageTags(BaseModel):
    tags: list[TagType] = Field(
        default_factory=list,
        description="Tags for the image, relate to the content or policy violations in the image. Do not include commentary or explanations. Must be lowercase and contain only alphanumeric characters and underscores",
    )
    error: str | None = Field(
        default=None,
        description="Brief reason in English why the tags could not be generated. Must be null unless tags failed for technical or content-related reasons",
    )


class ImageDescriptionTagResult(ImageDescription, ImageTags):
    usage_metadata: dict[str, UsageMetadata] = {}


class ImageDescriptionTagProcessor:
    def __init__(
        self,
        *,
        image_desription_prompt: str,
        image_tag_prompt: str,
        llms: list[BaseChatModel],
    ) -> None:
        self._llms = llms
        self._image_desription_prompt = image_desription_prompt
        self._image_tag_prompt = image_tag_prompt

    @staticmethod
    def _image_msg(prompt_text: str, file_path: str) -> HumanMessage:
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

    def _describe(self, file_path: str) -> ImageDescription | None:
        res, builder_index, provider, err = structured_llm_fallback(
            state={"file_path": file_path},
            llms=self._llms,
            schema=ImageDescription,
            build_messages_list=[lambda s: self._image_msg(self._image_desription_prompt, s["file_path"])],
        )
        if err:
            raise err
        return res

    def _tags(self, file_path: str) -> ImageTags | None:
        res, builder_index, provider, err = structured_llm_fallback(
            state={"file_path": file_path},
            llms=self._llms,
            schema=ImageTags,
            build_messages_list=[lambda s: self._image_msg(self._image_tag_prompt, s["file_path"])],
        )
        if err:
            raise err
        return res

    def run(self, file_path: str) -> ImageDescriptionTagResult:
        with get_usage_metadata_callback() as cb:
            desc_result = self._describe(file_path)
            tags_result = self._tags(file_path)
            final_result = ImageDescriptionTagResult(
                description=desc_result.description if desc_result else None,
                summary=desc_result.summary if desc_result else None,
                tags=tags_result.tags if tags_result else [],
                usage_metadata=cb.usage_metadata,
            )
        return final_result

    def batch(self, file_paths: list[str]) -> list[ImageDescriptionTagResult]:
        results = []
        for file_path in file_paths:
            results.append(self.run(file_path))
        return results


if __name__ == "__main__":
    from llm_orchestrator.llm_helpers import init_chat_model

    llms = [init_chat_model("gpt-4o-mini"), init_chat_model("google_genai:gemini-2.5-flash")]
    processor = ImageDescriptionTagProcessor(
        image_desription_prompt="Describe the image",
        image_tag_prompt="Tag the image",
        llms=llms,
    )
    # Example usage:
    result = processor.run("/Users/datn/Desktop/Screenshot 2025-07-09 at 12.19.52.png")
    print(result)
