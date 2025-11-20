from openai import AsyncOpenAI
from openai.types.audio.transcription_verbose import TranscriptionVerbose


class AudioTranscribeResult(TranscriptionVerbose):
    """override some attributes to make it optional, might be a bug in the openai library"""

    language: str | None


class AudioTranscribe:
    def __init__(self, model: str = "whisper-1", openai_key: str | None = None):
        self.client = AsyncOpenAI(api_key=openai_key)
        self.model = model

    async def abatch(self, audio_paths: list[str]) -> list[AudioTranscribeResult]:
        results = []
        for audio_path in audio_paths:
            with open(audio_path, "rb") as f:
                resp = await self.client.audio.transcriptions.create(
                    model=self.model,
                    file=f,
                    response_format="verbose_json",
                    timestamp_granularities=["segment"],
                )
            results.append(AudioTranscribeResult.model_validate(resp.model_dump()))
        return results


if __name__ == "__main__":
    import asyncio
    import os

    from dotenv import load_dotenv

    load_dotenv()

    transcriber = AudioTranscribe(model="whisper-1", openai_key=os.getenv("OPENAI_API_KEY"))
    results = asyncio.run(transcriber.abatch(["../../tests/fixtures/telegram_video_G_L_Rockwell_17754_audio.wav"]))
    print(results)
