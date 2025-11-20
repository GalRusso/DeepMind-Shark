"""Output models for each task's results and a global TaskKey→Model map."""

from constants import TaskKey
from llm_orchestrator.audio_transcribe import AudioTranscribeResult
from llm_orchestrator.audio_translate import TranslateResult
from llm_orchestrator.image_abuse_profiler import ImageAbuseProfilerResult
from llm_orchestrator.image_des_tag import ImageDescriptionTagResult
from llm_orchestrator.image_embedding import ImageEmbeddingResult
from llm_orchestrator.video_abuse_profiler import VideoAbuseFrameResult
from llm_orchestrator.video_des_tag import VideoDescriptionTagResult
from llm_orchestrator.video_frame_abuse_consolidation import VideoProfileResult
from pydantic import BaseModel


class AudioResult(BaseModel):
    audio_path: str | None


class VideoExtractFramesResult(BaseModel):
    frames: list[str]
    duration: float
    width: int
    height: int


VideoEmbeddingResult = ImageEmbeddingResult


class MaybeAudioTranscribeResult(BaseModel):
    result: AudioTranscribeResult | None
    reason: str | None = None


# Global mapping from TaskKey to its canonical output model
TASK_OUTPUT_MODEL_MAP: dict[TaskKey, type[BaseModel]] = {
    TaskKey.image_des_tag: ImageDescriptionTagResult,
    TaskKey.image_abuse: ImageAbuseProfilerResult,
    TaskKey.image_embedding: ImageEmbeddingResult,
    TaskKey.video_extract_frames: VideoExtractFramesResult,
    TaskKey.video_des_tag: VideoDescriptionTagResult,
    TaskKey.video_embedding: VideoEmbeddingResult,  # placeholder if used later
    TaskKey.video_frame_abuse: VideoAbuseFrameResult,
    TaskKey.video_frame_abuse_consolidate: VideoProfileResult,
    TaskKey.video_extract_audio: AudioResult,
    TaskKey.video_audio_transcribe: MaybeAudioTranscribeResult,
    TaskKey.video_audio_translate: TranslateResult,
}
