import base64
import mimetypes
from typing import Literal

from langchain_core.callbacks import UsageMetadataCallbackHandler, get_usage_metadata_callback
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage
from langchain_core.messages.ai import UsageMetadata
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph
from llm_orchestrator.llm_helpers import structured_llm_fallback
from prompt_profiles_config import ImageAbuseCategory
from pydantic import BaseModel, Field


class ImageState(BaseModel):
    file_path: str
    tags: list[str] = []
    description: str | None = None
    category_name: str | None = None
    is_category: bool | None = None
    cat_conf: int | None = None
    cat_reason: str | None = None
    adv_level: str | None = None
    adv_conf: int | None = None
    adv_reason: str | None = None


class AbuseCheckOutput(BaseModel):
    is_category: bool = Field(description="Indicates whether the content falls under the specified category.")
    category: str | None = Field(description="The name of the category being evaluated (null if not applicable).")
    confidence: int = Field(description="An integer from 0 to 100 estimating how certain the classification is")
    reasoning: str | None = Field(
        description="a concise justification in English (max 150 characters), explaining **why the category does or does not apply**."
    )
    error: str | None = Field(
        default=None,
        description="Brief reason in English why the category could not be determined. Must be null unless classification failed for technical or content-related reasons",
    )


class AdversaryLevelOutput(BaseModel):
    adversary_level: Literal["non_adversarial", "semi_adversarial", "fully_adversarial"] | None = Field(
        description="The adversary level of the content, must be one of: non_adversarial, semi_adversarial, fully_adversarial"
    )
    confidence: int = Field(description="An integer between 0 and 100 estimating how certain the classification is.")
    reasoning: str | None = Field(
        description="A brief, specific explanation (max 150 characters including spaces) of why the content is at the specified adversary level."
    )
    error: str | None = Field(
        default=None,
        description="Brief reason in English why the level could not be determined. Must be null unless classification failed for technical or content-related reasons",
    )


class ImageAbuseProfilerResult(ImageState):
    usage_metadata: dict[str, UsageMetadata] = {}  # usage metadata for the whole batch


class ImageAbuseProfiler:
    def __init__(
        self,
        abuse_category: ImageAbuseCategory,
        llms: list[BaseChatModel],
    ) -> None:
        self._abuse_category = abuse_category
        self._llms = llms
        self._app = None

    # ---- prompt helpers ----
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

    def _category_prompt(self, tags: list[str], description: str, file_path: str) -> HumanMessage:
        _prompt = self._abuse_category.format(tags, description)
        return self._image_msg(_prompt, file_path)

    def _category_prompt_without_image(self, tags: list[str], description: str) -> HumanMessage:
        _prompt = self._abuse_category.format(tags, description)
        return HumanMessage(
            content=[
                {"type": "text", "text": _prompt},
            ]
        )

    def _adversary_prompt(self, tags: list[str], description: str, file_path: str) -> HumanMessage:
        _prompt = self._abuse_category.format_adversary_levels(tags, description)
        return self._image_msg(_prompt, file_path)

    def _adversary_prompt_without_image(self, tags: list[str], description: str) -> HumanMessage:
        _prompt = self._abuse_category.format_adversary_levels(tags, description)

        return HumanMessage(
            content=[
                {"type": "text", "text": _prompt},
            ]
        )

    def _category_node(self, state: ImageState) -> dict:
        res, builder_index, provider, err = structured_llm_fallback(
            state=state,
            llms=self._llms,
            schema=AbuseCheckOutput,
            build_messages_list=[
                lambda st: self._category_prompt(st.tags, st.description or "", st.file_path),
                lambda st: self._category_prompt_without_image(st.tags, st.description or ""),
            ],
        )
        if isinstance(res, AbuseCheckOutput):
            return {
                "category_name": res.category or self._abuse_category.category_name,
                "is_category": bool(res.is_category),
                "cat_conf": int(res.confidence or 0),
                "cat_reason": res.reasoning or "",
            }
        return {
            "category_name": None,
            "is_category": False,
            "cat_reason": str(err) if err else f"no provider succeeded ({provider})",
        }

    def _adversary_node(self, state: ImageState) -> dict:
        res, builder_index, provider, err = structured_llm_fallback(
            state=state,
            llms=self._llms,
            schema=AdversaryLevelOutput,
            build_messages_list=[
                lambda st: self._adversary_prompt(st.tags, st.description or "", st.file_path),
                lambda st: self._adversary_prompt_without_image(st.tags, st.description or ""),
            ],
        )
        if isinstance(res, AdversaryLevelOutput) and res.adversary_level:
            return {
                "adv_level": res.adversary_level,
                "adv_conf": int(res.confidence or 0),
                "adv_reason": res.reasoning or "",
            }
        return {"adv_reason": str(err) if err else f"no provider succeeded ({provider})"}

    def build(self):
        sub = StateGraph(ImageState)

        sub.add_node("category", self._category_node)
        sub.add_node("adversary", self._adversary_node)

        sub.add_edge(START, "category")

        def route_category(s: ImageState) -> str:
            if s.is_category is True:
                return "adversary"
            return END

        sub.add_conditional_edges(
            "category",
            route_category,
        )

        sub.add_edge("adversary", END)
        self._app = sub.compile()
        return self._app

    def run(
        self,
        file_path: str,
        description: str | None = None,
        tags: list[str] | None = None,
    ) -> ImageAbuseProfilerResult:
        app = self._app or self.build()

        with get_usage_metadata_callback() as cb:
            result = app.invoke(
                ImageState(
                    file_path=file_path,
                    description=description,
                    tags=tags or [],
                ),
            )
            final_result = ImageAbuseProfilerResult(**result, usage_metadata=cb.usage_metadata)
        return final_result

    def batch(
        self,
        file_paths: list[str],
        descriptions: list[str | None] | None = None,
        tagss: list[list[str] | None] | None = None,
    ) -> list[ImageAbuseProfilerResult]:
        app = self._app or self.build()

        states = []
        for file_path, description, tags in zip(file_paths, descriptions, tagss, strict=False):
            states.append(
                ImageState(
                    file_path=file_path,
                    description=description,
                    tags=tags or [],
                )
            )

        final_results = []

        callbacks = [UsageMetadataCallbackHandler() for _ in range(len(states))]
        configs = [RunnableConfig(callbacks=[cb]) for cb in callbacks]
        results = app.batch(states, config=configs)
        for result, cb in zip(results, callbacks, strict=True):
            final_results.append(ImageAbuseProfilerResult(**result, usage_metadata=cb.usage_metadata))
        return final_results


# For testing
if __name__ == "__main__":
    from llm_orchestrator.llm_helpers import init_chat_model
    from prompt_profiles_config import AdversaryLevel, PTImageDesTag

    abuse_category = ImageAbuseCategory(
        version=1,
        category_name="sample",
        models=["gpt-4o-mini", "google_genai:gemini-2.5-flash"],
        pt_image_desc_tag=PTImageDesTag(
            version=1,
            description_prompt_template="Sample prompt",
            tag_prompt_template="Sample tag prompt",
            models=["gpt-4o-mini", "google_genai:gemini-2.5-flash"],
        ),
        abuse_category_prompt_template="Sample abuse prompt",
        adversary_levels_prompt_template="Sample adversary prompt",
        category_definition="Sample definition",
        adversary_levels=[
            AdversaryLevel(name="low", prompt_template="Low level def", examples=None),
            AdversaryLevel(name="high", prompt_template="High level def", examples=None),
        ],
    )
    print(abuse_category.format())
    print(abuse_category.format_adversary_levels())

    llms = [init_chat_model("gpt-4o-mini"), init_chat_model("google_genai:gemini-2.5-flash")]
    profiler = ImageAbuseProfiler(abuse_category=abuse_category, llms=llms)
    result = profiler.run("/Users/datn/Desktop/Screenshot 2025-07-09 at 12.19.52.png", None, None)
    print(result)

    # assert profiler._app is not None
    # profiler._app.get_graph().draw_mermaid_png(output_file_path="docs/abuse_graph.png")
