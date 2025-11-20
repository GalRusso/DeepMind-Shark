import hashlib
import os
from enum import Enum
from typing import Annotated, Any

from langchain_core.prompts import PromptTemplate as LangchainPromptTemplate
from pydantic import BaseModel, BeforeValidator, PlainSerializer, computed_field, field_validator, model_serializer

Tag = str

# Robust path resolution for both local and Databricks execution
_current_dir = os.getcwd()
_script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else _current_dir
FIXTURE_DIR = os.path.abspath(os.path.join(_script_dir, "../../fixtures"))


def load_prompt_template(prompt_template_md_file: str | LangchainPromptTemplate | Any) -> LangchainPromptTemplate:
    """prompt_template_md_file: file path relative to fixtures/prompt_template/"""
    if isinstance(prompt_template_md_file, LangchainPromptTemplate):
        return prompt_template_md_file
    if not isinstance(prompt_template_md_file, str):
        raise ValueError(f"Invalid prompt template: {prompt_template_md_file}")

    if not prompt_template_md_file.endswith(".md"):
        return LangchainPromptTemplate.from_template(prompt_template_md_file)
    with open(f"{FIXTURE_DIR}/prompt_template/{prompt_template_md_file}") as f:
        return LangchainPromptTemplate.from_template(f.read())


def serialize_prompt_template(prompt_template: LangchainPromptTemplate) -> str:
    return prompt_template.template


CustomPromptTemplate = Annotated[
    LangchainPromptTemplate, BeforeValidator(load_prompt_template), PlainSerializer(serialize_prompt_template)
]


class StaticPromptBase(BaseModel):
    version: int
    # #https://python.langchain.com/api_reference/langchain/chat_models/langchain.chat_models.base.init_chat_model.html
    models: list[str]  # e.g openai:gpt-4o-mini, gemini:gemini-2.0-flash-001,

    def hash(self) -> str:
        return hashlib.blake2b(self.model_dump_json().encode("utf-8")).hexdigest()


class PTImageDesTag(StaticPromptBase):
    description_prompt_template: CustomPromptTemplate
    tag_prompt_template: CustomPromptTemplate

    def __hash__(self):
        return hash(
            (
                self.version,
                *self.models,
                self.description_prompt_template.template,
                self.tag_prompt_template.template,
            )
        )


class PTVideoDesTag(StaticPromptBase):
    pt_video_desc: CustomPromptTemplate
    pt_video_tags: CustomPromptTemplate
    # fallback to frames
    pt_frames_desc: CustomPromptTemplate
    pt_frames_tags: CustomPromptTemplate
    pt_desc_summary: CustomPromptTemplate
    pt_tags_summary: CustomPromptTemplate
    frame_models: list[str]  # e.g openai:gpt-4o-mini, gemini:gemini-2.0-flash-001,


class PTTextTranslation(StaticPromptBase):
    prompt_template: CustomPromptTemplate
    models: list[str]  # e.g openai:gpt-4o-mini, gemini:gemini-2.0-flash-001,


class AdversaryLevelExample(BaseModel):
    description: str
    tags: list[Tag]
    image: bytes | None = None


class AdversaryLevel(BaseModel):
    name: str
    prompt_template: CustomPromptTemplate
    examples: list[AdversaryLevelExample] | None = None

    @model_serializer
    def format_prompt_template(self) -> dict:
        return {
            "name": self.name,
            "prompt_template": self.prompt_template.format(examples=self.examples),
        }

    def format(self) -> str:
        return self.model_dump_json(exclude={"examples"})


# Abuse Category depends on description & tagging, so it's not a PTImageDesTag
class BaseAbuseCategory(StaticPromptBase):
    version: int
    category_name: str
    abuse_category_prompt_template: CustomPromptTemplate
    adversary_levels_prompt_template: CustomPromptTemplate
    category_definition: CustomPromptTemplate
    adversary_levels: list[AdversaryLevel]

    def format(self, tags: list[str] | None = None, description: str | None = None) -> str:
        return self.abuse_category_prompt_template.format(
            category_name=self.category_name,
            category_definition=self.category_definition.format(),
            tags=", ".join(tags or []),
            description=description or "",
        )

    def format_adversary_levels(self, tags: list[str] | None = None, description: str | None = None) -> str:
        return self.adversary_levels_prompt_template.format(
            category_name=self.category_name,
            category_definition=self.category_definition.format(),
            tags=", ".join(tags or []),
            description=description or "",
            adversary_levels="\n".join(
                [f"{level.name}: {level.prompt_template.format()}" for level in self.adversary_levels]
            ),
        )


class ImageAbuseCategory(BaseAbuseCategory):
    pt_image_desc_tag: PTImageDesTag

    @computed_field
    def pt_image_desc_tag_hash(self) -> str:
        """This help identify this abuse category is depending on the prompt description tag template"""
        return self.pt_image_desc_tag.hash()

    @field_validator("pt_image_desc_tag", mode="before")
    @classmethod
    def load_prompt_desc_tag_template(cls, value: Any) -> PTImageDesTag:
        if isinstance(value, str) and value.endswith(".json"):
            with open(f"{FIXTURE_DIR}/{value}") as f:
                return PTImageDesTag.model_validate_json(f.read())
        return value


class VideoAbuseCategory(BaseAbuseCategory):
    pt_video_desc_tag: PTVideoDesTag

    @computed_field
    def pt_video_desc_tag_hash(self) -> str:
        """This help identify this abuse category is depending on the prompt description tag template"""
        return self.pt_video_desc_tag.hash()

    @field_validator("pt_video_desc_tag", mode="before")
    @classmethod
    def load_video_prompt_desc_tag_template(cls, value: Any) -> PTVideoDesTag:
        if isinstance(value, str) and value.endswith(".json"):
            with open(f"{FIXTURE_DIR}/{value}") as f:
                return PTVideoDesTag.model_validate_json(f.read())
        return value


class PromptProfile(BaseModel):
    name: str
    image_abuse_categories: list[ImageAbuseCategory] = []
    video_abuse_categories: list[VideoAbuseCategory] = []


class PromptTableType(str, Enum):
    image_description_tag = "image_description_tag"
    image_abuse_category = "image_abuse_category"
    video_description_tag = "video_description_tag"
    video_abuse_category = "video_abuse_category"
    text_translation = "text_translation"


with open(f"{FIXTURE_DIR}/image_description_tag.json") as f:
    P_IMAGE_DESCRIPTION = PTImageDesTag.model_validate_json(f.read())


with open(f"{FIXTURE_DIR}/profile_mvp.json") as f:
    P_PROFILE_MVP = PromptProfile.model_validate_json(f.read())

with open(f"{FIXTURE_DIR}/video_description_tag.json") as f:
    P_VIDEO_DESCRIPTION = PTVideoDesTag.model_validate_json(f.read())

with open(f"{FIXTURE_DIR}/text_translation.json") as f:
    P_TEXT_TRANSLATION = PTTextTranslation.model_validate_json(f.read())


# make sure serialize and deserialize are working
PTImageDesTag.model_validate_json(P_IMAGE_DESCRIPTION.model_dump_json()).hash()
PromptProfile.model_validate_json(P_PROFILE_MVP.model_dump_json()).image_abuse_categories[0].hash()
PTVideoDesTag.model_validate_json(P_VIDEO_DESCRIPTION.model_dump_json()).hash()
PTTextTranslation.model_validate_json(P_TEXT_TRANSLATION.model_dump_json()).hash()

if __name__ == "__main__":
    print(PTImageDesTag.model_validate_json(P_IMAGE_DESCRIPTION.model_dump_json()).hash())
    print(PromptProfile.model_validate_json(P_PROFILE_MVP.model_dump_json()).image_abuse_categories[0].hash())
    print(PTVideoDesTag.model_validate_json(P_VIDEO_DESCRIPTION.model_dump_json()).hash())
    print(PTTextTranslation.model_validate_json(P_TEXT_TRANSLATION.model_dump_json()).hash())
