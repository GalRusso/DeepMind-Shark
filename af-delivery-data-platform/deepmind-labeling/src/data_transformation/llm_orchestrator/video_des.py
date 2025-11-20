import os
from typing import Literal, Self

from langchain_core.callbacks import UsageMetadataCallbackHandler
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage
from langchain_core.messages.ai import UsageMetadata
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph
from llm_orchestrator.llm_helpers import astructured_llm_fallback, construct_gemini_video_msg, construct_image_msg
from llm_orchestrator.video_helpers import upload_and_wait_active
from loguru import logger
from prompt_profiles_config import PTVideoDesTag
from pydantic import BaseModel, Field


class _VideoDescription(BaseModel):
    description: str | None = Field(
        default=None,
        description="A single paragraph of up to 200 words describing the content, written in natural English without referencing the original format (e.g., video or image).",
    )
    summary: str | None = Field(default=None, description="A one-line summary in 12 words or fewer in English.")
    error: str | None = Field(
        default=None,
        description="Brief reason why the content could not be processed in English. When the content cannot be processed, set this field to the reason.",
    )

    def good(self) -> bool:
        if self.error:
            return False
        return all([self.description, self.summary])


class VideoDescState(BaseModel):
    file_path: str
    frames_path: list[str] | None = None
    uploaded_file_info: dict | None = None
    inline_result: _VideoDescription | None = None
    upload_result: _VideoDescription | None = None
    frames_result: list[_VideoDescription] | None = None
    summary_result: _VideoDescription | None = None


class VideoDescResult(_VideoDescription):
    result_from: Literal["upload", "inline", "summary"] | None = None
    usage_metadata: dict[str, UsageMetadata] = Field(default_factory=dict)

    @classmethod
    def from_state(cls, state: VideoDescState, usage_metadata: dict[str, UsageMetadata]) -> Self:
        if state.upload_result and state.upload_result.good():
            return cls(
                **state.upload_result.model_dump(),
                usage_metadata=usage_metadata,
                result_from="upload",
            )
        if state.inline_result and state.inline_result.good():
            return cls(
                **state.inline_result.model_dump(),
                usage_metadata=usage_metadata,
                result_from="inline",
            )
        if state.summary_result and state.summary_result.good():
            return cls(
                **state.summary_result.model_dump(),
                usage_metadata=usage_metadata,
                result_from="summary",
            )
        return cls(
            usage_metadata=usage_metadata,
            error="No result found. Inline error: {state.inline_result.error}. Upload error: {state.upload_result.error}. Summary error: {state.summary_result.error}",
        )


class VideoDescProcessor:
    def __init__(
        self,
        *,
        prompt: PTVideoDesTag,
        llms: list[BaseChatModel],
        fallback_llms: list[BaseChatModel] | None = None,
    ) -> None:
        """
        fallback_* is used for frames description
        """
        self._llms = llms
        self._prompt = prompt
        self._fallback_llms = fallback_llms or llms
        self._app = None

    async def _describe_inline(self, state: VideoDescState) -> dict:
        """describe the video by add the video inline to prompt"""
        logger.info("describe_inline")
        res, builder_index, provider, err = await astructured_llm_fallback(
            state=state,
            llms=self._llms,
            schema=_VideoDescription,
            build_messages_list=[
                lambda s: construct_gemini_video_msg(
                    self._prompt.pt_video_desc.format(),
                    s.file_path,
                )
            ],
        )
        if err:
            logger.error(f"describe_inline error: {err}")
        result = res.model_dump() if res else {}
        return {"inline_result": result}

    async def _describe_upload(self, state: VideoDescState) -> dict:
        """describe the video by upload the video to google cloud storage"""
        logger.info("describe_upload")

        res, builder_index, provider, err = await astructured_llm_fallback(
            state=state,
            llms=self._llms,
            schema=_VideoDescription,
            build_messages_list=[
                lambda s: construct_gemini_video_msg(
                    self._prompt.pt_video_desc.format(),
                    s.file_path,
                    uploaded_file_info=s.uploaded_file_info,
                )
            ],
        )
        if err:
            logger.error(f"describe_upload error: {err}")
        result = res.model_dump() if res else {}
        # result["error"] = str(err) if err else None
        return {"upload_result": result}

    async def _describe_frames(self, state: VideoDescState) -> dict:
        logger.info("describe_frames")
        if not state.frames_path:
            return {}

        frames_result = []
        for frame_path in state.frames_path:
            res, builder_index, provider, err = await astructured_llm_fallback(
                state={"file_path": frame_path},
                llms=self._fallback_llms,
                schema=_VideoDescription,
                build_messages_list=[
                    lambda s: construct_image_msg(
                        self._prompt.pt_video_desc.format(),
                        s["file_path"],
                    )
                ],
            )
            if err:
                logger.error(f"describe_frames error: {err}")
            if res:
                frames_result.append(res.model_dump())
        logger.info(f"describe_frames result: {frames_result}")
        return {"frames_result": frames_result}

    async def _summary_describe_frames(self, state: VideoDescState) -> dict:
        logger.info("summary_describe_frames")
        if not state.frames_result:
            return {}
        descriptions = [frame.description for frame in state.frames_result]
        summaries = [frame.summary for frame in state.frames_result]
        message = HumanMessage(
            content=[
                {
                    "type": "text",
                    "text": self._prompt.pt_desc_summary.format(
                        descriptions=descriptions,
                        summaries=summaries,
                    ),
                },
            ]
        )
        logger.info(f"summary_describe_frames message: {message}")
        res, builder_index, provider, err = await astructured_llm_fallback(
            state={"message": message},
            llms=self._fallback_llms,
            schema=_VideoDescription,
            build_messages_list=[lambda s: s["message"]],
        )
        if err:
            logger.error(f"summary_describe_frames error: {err}")
        logger.debug(f"summary_describe_frames result: {res.model_dump() if res else {}}")
        if not res:
            return {}
        return {"summary_result": res.model_dump()}

    def _build(self):
        sub = StateGraph(VideoDescState)
        sub.add_node("describe_upload", self._describe_upload)
        sub.add_node("describe_inline", self._describe_inline)
        sub.add_node("describe_frames", self._describe_frames)
        sub.add_node("describe_frames_summary", self._summary_describe_frames)

        sub.add_conditional_edges(
            START,
            lambda s: "has_upload" if s.uploaded_file_info else "no_upload",
            path_map={"has_upload": "describe_upload", "no_upload": "describe_inline"},
        )

        sub.add_conditional_edges(
            "describe_upload",
            lambda s: "fail" if not s.upload_result.good() else "ok",
            path_map={"fail": "describe_inline", "ok": END},
        )

        sub.add_conditional_edges(
            "describe_inline",
            lambda s: "fail" if not s.inline_result.good() else "ok",
            path_map={"fail": "describe_frames", "ok": END},
        )
        sub.add_edge("describe_frames", "describe_frames_summary")
        sub.add_edge("describe_frames_summary", END)
        self._app = sub.compile()
        return self._app

    async def abatch(
        self,
        file_paths: list[str],
        uploaded_file_infos: list[dict] | None = None,
        frames_paths: list[list[str]] | None = None,
    ) -> list[VideoDescResult]:
        app = self._app or self._build()

        states = []
        if not uploaded_file_infos:
            uploaded_file_infos = [{} for _ in range(len(file_paths))]
        if not frames_paths:
            frames_paths = [[] for _ in range(len(file_paths))]
        for file_path, uploaded_file_info, frames_path in zip(
            file_paths, uploaded_file_infos, frames_paths, strict=True
        ):
            states.append(
                VideoDescState(file_path=file_path, uploaded_file_info=uploaded_file_info, frames_path=frames_path)
            )

        final_results = []

        callbacks = [UsageMetadataCallbackHandler() for _ in range(len(states))]
        configs = [RunnableConfig(callbacks=[cb]) for cb in callbacks]
        results = await app.abatch(states, config=configs)
        for result, cb in zip(results, callbacks, strict=True):
            result = VideoDescState.model_validate(result)
            final_results.append(VideoDescResult.from_state(result, cb.usage_metadata))
        return final_results


if __name__ == "__main__":
    import asyncio
    import logging

    from dotenv import load_dotenv
    from langchain.chat_models import init_chat_model

    # Ensure logs are output to the console
    logging.basicConfig(level=logging.INFO)

    load_dotenv()

    llm = init_chat_model("google_genai:gemini-2.5-flash")

    logger = logging.getLogger(__name__)

    processor = VideoDescProcessor(
        prompt=PTVideoDesTag(
            version=1,
            models=["google_genai:gemini-2.5-flash"],
            pt_video_desc="Describe the video",
            pt_video_tags="Tag the video",
            pt_frames_desc="Describe the frames",
            pt_frames_tags="Tag the frames",
            pt_desc_summary="Summarize the descriptions {descriptions} and the summeries {summaries}",
            pt_tags_summary="Summarize the tags {tags}",
            frame_models=["google_genai:gemini-2.5-flash"],
        ),
        llms=[llm],
        logger=logger,
    )
    # draw graph
    processor._build()
    processor._app.get_graph().draw_mermaid_png(output_file_path="../../docs/video_desc_graph.png")

    EXAMPLE_VIDEO_PATH = "../../tests/fixtures/BigBuckBunny_320x180_trimmed.mp4"

    # Example usage:
    async def test_upload():
        upfile = await upload_and_wait_active(EXAMPLE_VIDEO_PATH, os.getenv("GEMINI_API_KEY") or "")
        result = await processor.abatch([EXAMPLE_VIDEO_PATH], [upfile])
        print("test_upload", result)

    async def test_inline():
        result = await processor.abatch([EXAMPLE_VIDEO_PATH])
        print("test_inline", result)

    async def test_frames():
        result = await processor.abatch(
            ["not_a_real_video.mp4"],
            frames_paths=[
                [
                    "../../tests/fixtures/BigBuckBunny_320x180_extracted_frame_223d483cc6c2483aa13f2274069a96c0.png",
                    "../../tests/fixtures/BigBuckBunny_320x180_extracted_frame_cf1ad678bf8640c582d3928623be05f6.png",
                    "../../tests/fixtures/BigBuckBunny_320x180_extracted_frame_da29813b7615439d950a98cec29c2963.png",
                ]
            ],
        )
        print("test_frames", result)

    async def main():
        await test_upload()
        await test_inline()
        await test_frames()

    result = asyncio.run(main())
