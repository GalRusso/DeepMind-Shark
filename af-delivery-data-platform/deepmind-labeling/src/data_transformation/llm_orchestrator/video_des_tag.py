import os

from langchain_core.language_models.chat_models import BaseChatModel
from llm_orchestrator.video_des import VideoDescProcessor, VideoDescResult
from llm_orchestrator.video_helpers import upload_and_wait_active
from llm_orchestrator.video_tag import VideoTagProcessor, VideoTagResult
from loguru import logger
from prompt_profiles_config import PTVideoDesTag
from pydantic import BaseModel


class VideoDescriptionTagResult(BaseModel):
    desc_result: VideoDescResult
    tags_result: VideoTagResult


class VideoDescriptionTagProcessor:
    def __init__(
        self,
        *,
        prompt: PTVideoDesTag,
        llms: list[BaseChatModel],
        fallback_llms: list[BaseChatModel] | None = None,
        gemini_api_key: str | None = None,
    ) -> None:
        """gemini_api_key is used to upload the video to google cloud storage"""

        self._llms = llms
        self._prompt = prompt
        self._desc_processor = VideoDescProcessor(
            prompt=prompt,
            llms=llms,
            fallback_llms=fallback_llms,
        )
        self._tag_processor = VideoTagProcessor(
            prompt=prompt,
            llms=llms,
            fallback_llms=fallback_llms,
        )
        self._gemini_api_key = gemini_api_key

    async def abatch(
        self,
        file_paths: list[str],
        frames_paths: list[list[str]] | None = None,
    ) -> list[VideoDescriptionTagResult]:
        uploaded_file_infos = []
        for file_path in file_paths:
            if not self._gemini_api_key:
                uploaded_file_infos.append({})
                continue
            try:
                upfile = await upload_and_wait_active(file_path, self._gemini_api_key)
            except Exception as e:
                logger.error(f"Error uploading file {file_path}: {e}")
                upfile = {}
            uploaded_file_infos.append(upfile)

        desc_results = await self._desc_processor.abatch(file_paths, uploaded_file_infos, frames_paths)
        tag_results = await self._tag_processor.abatch(file_paths, uploaded_file_infos, frames_paths)
        final_results = []
        for desc_result, tag_result in zip(desc_results, tag_results, strict=True):
            final_results.append(VideoDescriptionTagResult(desc_result=desc_result, tags_result=tag_result))
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

    processor = VideoDescriptionTagProcessor(
        prompt=PTVideoDesTag(
            version=1,
            models=["google_genai:gemini-2.5-flash"],
            pt_video_desc="Describe the video",
            pt_video_tags="Tag the video",
            pt_frames_desc="Describe the frames",
            pt_frames_tags="Tag the frames",
            pt_desc_summary="Summarize the descriptions {descriptions} and the summeries {summaries}",
            pt_tags_summary="Consolidate and summarize this tags into a clean, concise list of up to 15 distinct tags.<tag>{tags}</tag>",
            frame_models=["google_genai:gemini-2.5-flash"],
        ),
        llms=[llm],
        logger=logger,
        gemini_api_key=os.getenv("GEMINI_API_KEY") or "",
    )
    EXAMPLE_VIDEO_PATH = "../../tests/fixtures/BigBuckBunny_320x180_trimmed.mp4"

    # Example usage:
    async def test_inline_or_upload():
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
        await test_inline_or_upload()
        await test_frames()

    result = asyncio.run(main())
