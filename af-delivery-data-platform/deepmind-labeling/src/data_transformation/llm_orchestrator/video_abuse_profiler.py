from llm_orchestrator.image_abuse_profiler import ImageAbuseProfilerResult
from pydantic import BaseModel

FrameAbuseProfilerResult = ImageAbuseProfilerResult


class VideoAbuseFrameResult(BaseModel):
    video_file: str
    frame_results: list[FrameAbuseProfilerResult]
