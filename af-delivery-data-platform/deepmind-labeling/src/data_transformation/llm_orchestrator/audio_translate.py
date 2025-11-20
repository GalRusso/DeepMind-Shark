import json

from langchain_core.callbacks import get_usage_metadata_callback
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage
from langchain_core.messages.ai import UsageMetadata
from llm_orchestrator.llm_helpers import astructured_llm_fallback
from loguru import logger
from prompt_profiles_config import PTTextTranslation
from pydantic import BaseModel, Field


class TranslateSegmentResult(BaseModel):
    text: str | None = Field(default=None, description="The translated text in English")
    error: str | None = Field(default=None, description="The error explanation if the translation failed")


class TranslateResult(BaseModel):
    translations: list[TranslateSegmentResult] = Field(
        default_factory=list,
        description="The translated structured output, each items corresponds to the input line by position",
    )
    usage_metadata: dict[str, UsageMetadata] = Field(default_factory=dict)


class AudioTranslate:
    def __init__(self, prompt: PTTextTranslation, llms: list[BaseChatModel]):
        self.llms = llms
        self.prompt = prompt

    async def batch(self, audio_paths: list[str]) -> list[str]: ...

    def construct_message(self, transcript_lines: list[str]) -> HumanMessage:
        parts: list[dict] = [
            {
                "type": "text",
                "text": self.prompt.prompt_template.format(transcript_lines=json.dumps(transcript_lines, indent=2)),
            }
        ]
        return HumanMessage(content=parts)

    async def ainvoke(self, transcript_lines: list[str], batch_size: int = 15) -> TranslateResult:
        translated_texts = []

        for i in range(0, len(transcript_lines), batch_size):
            batch = transcript_lines[i : i + batch_size]
            res, builder_index, provider, err = await astructured_llm_fallback(
                state={"transcript_lines": batch},
                llms=self.llms,
                schema=TranslateResult,
                build_messages_list=[
                    lambda s: self.construct_message(s["transcript_lines"]),
                ],
            )
            if err:
                logger.error(f"tagging_inline error: {err}")
            if res:
                translated_texts.extend(res.translations)
        return TranslateResult(translations=translated_texts)

    async def abatch(self, transcript_liness: list[list[str]]) -> list[TranslateResult]:
        results = []
        for transcript_lines in transcript_liness:
            with get_usage_metadata_callback() as cb:
                _ret = await self.ainvoke(transcript_lines)
                _ret.usage_metadata = cb.usage_metadata
                results.append(_ret)
        return results


if __name__ == "__main__":
    import asyncio
    import os

    from dotenv import load_dotenv
    from langchain.chat_models import init_chat_model
    from llm_orchestrator.audio_transcribe import AudioTranscribe
    from prompt_profiles_config import P_TEXT_TRANSLATION

    load_dotenv()

    transcriber = AudioTranscribe(model="whisper-1", openai_key=os.getenv("OPENAI_API_KEY"))
    results = asyncio.run(transcriber.abatch(["../../tests/fixtures/telegram_video_G_L_Rockwell_17754_audio.wav"]))
    print(results)
    print("--------------------------------")
    print(len(results))

    transcript = results[0]
    lines = [seg.text for seg in transcript.segments] if transcript.segments else [transcript.text]

    llm = init_chat_model("google_genai:gemini-2.5-flash")
    translator = AudioTranslate(prompt=P_TEXT_TRANSLATION, llms=[llm])
    results = asyncio.run(translator.ainvoke(lines))
    print(results)
