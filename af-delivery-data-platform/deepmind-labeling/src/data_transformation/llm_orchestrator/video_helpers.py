import asyncio
import logging
import os
import shutil
import subprocess
import tempfile
import time
import uuid

import cv2
import ffmpeg
import numpy as np
from imageio_ffmpeg import count_frames_and_secs, get_ffmpeg_exe

logger = logging.getLogger(__name__)


def extract_frames(video_path: str, amount: int = 3, output_dir: str | None = None) -> tuple[list[str] | None, float]:
    frames: list[str] = []
    start_time = time.time()

    count, duration_value = count_frames_and_secs(video_path)
    if not count or not duration_value:
        logger.warning("count_frames_and_secs returned non-positive duration.")
        return None, time.time() - start_time

    exe_path = get_ffmpeg_exe()

    eps = 0.05
    timestamps = np.linspace(duration_value * 0.15, duration_value * 0.85, amount)
    timestamps = [max(0.0, min(float(ts), max(0.0, duration_value - eps))) for ts in timestamps]

    output_dir = output_dir or os.path.dirname(video_path)
    base_filename = os.path.splitext(os.path.basename(video_path))[0]

    def _run(cmd):
        return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    for ts in timestamps:
        out_filename = f"{base_filename}_extracted_frame_{uuid.uuid4().hex}.png"
        out_path = os.path.join(output_dir, out_filename)
        os.makedirs(output_dir, exist_ok=True)

        cmd1 = [
            exe_path,
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            video_path,
            "-ss",
            f"{ts:.3f}",
            "-map",
            "0:v:0",
            "-frames:v",
            "1",
            "-f",
            "image2",
            out_path,
        ]
        r = _run(cmd1)
        if r.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            frames.append(out_path)
            continue

        cmd2 = [
            exe_path,
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-ss",
            f"{ts:.3f}",
            "-i",
            video_path,
            "-map",
            "0:v:0",
            "-frames:v",
            "1",
            "-f",
            "image2",
            out_path,
        ]
        r = _run(cmd2)
        if r.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            frames.append(out_path)
            continue

        cmd3 = [
            exe_path,
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            video_path,
            "-vf",
            f"select=gte(t\\,{ts:.3f})",
            "-vsync",
            "vfr",
            "-frames:v",
            "1",
            out_path,
        ]
        r = _run(cmd3)
        if r.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            frames.append(out_path)
            continue

        logger.error(f"FFmpeg failed at {ts:.2f}s: {r.stderr.strip()}")

    return (frames if frames else None), time.time() - start_time


def get_chunks(video_path: str, max_chunk_size_mb: int, start_percents: list[int] | None = None) -> list[str]:
    if start_percents is None:
        start_percents = [25, 50, 75]
    output_chunks = []
    max_chunk_size_bytes = max_chunk_size_mb * 1024 * 1024
    MIN_DURATION = 3.0

    count, total_duration = count_frames_and_secs(video_path)
    assert total_duration is not None, "Could not parse duration from ffprobe output"

    total_size = os.path.getsize(video_path)
    bytes_per_second = total_size / total_duration

    output_dir = "/tmp"
    base_name = os.path.splitext(os.path.basename(video_path))[0]

    for percent in start_percents:
        chunk_filename = f"{base_name}_chunk_{uuid.uuid4().hex}.mp4"
        chunk_path = os.path.join(output_dir, chunk_filename)
        try:
            start_byte = int((percent / 100) * total_size)
            chunk_size = min(max_chunk_size_bytes, total_size - start_byte)
            if chunk_size <= 0:
                continue

            start_time = (percent / 100.0) * total_duration
            duration = max_chunk_size_bytes / bytes_per_second

            if duration < MIN_DURATION:
                continue

            TARGET_BITS = int(max_chunk_size_bytes * 8 * 0.97)
            AUDIO_K = 128_000
            VIDEO_K = max(50_000, TARGET_BITS // duration - AUDIO_K)

            (
                ffmpeg.input(video_path, ss=start_time)
                .output(
                    chunk_path,
                    t=duration,
                    vcodec="libx264",
                    acodec="aac",
                    video_bitrate=VIDEO_K,
                    audio_bitrate=AUDIO_K,
                    maxrate=VIDEO_K,
                    bufsize=VIDEO_K * 2,
                    preset="veryfast",
                    movflags="+faststart",
                )
                .overwrite_output()
                .run(capture_stdout=True, capture_stderr=True, cmd=get_ffmpeg_exe())
            )

            if os.path.exists(chunk_path) and os.path.getsize(chunk_path) > 0:
                output_chunks.append(chunk_path)
            else:
                logger.warning(f"Chunk not created or empty: {chunk_path}")
                if os.path.exists(chunk_path):
                    os.remove(chunk_path)

        except ffmpeg.Error as error:
            logger.error(f"FFmpeg chunk extraction failed: {error.stderr.decode(errors='ignore')}")
            if os.path.exists(chunk_path):
                os.remove(chunk_path)
        except Exception as e:
            logger.exception(f"Unexpected error while extracting chunk at {percent}%: {e!s}")
            if os.path.exists(chunk_path):
                os.remove(chunk_path)

    return output_chunks


async def upload_and_wait_active(local_file_path: str, api_key: str) -> dict:
    """Upload a file to Google Cloud Storage and wait until it is ACTIVE,
    return a model_dump of the File object
    """
    from google import genai

    client = genai.Client(api_key=api_key)

    assert os.path.exists(local_file_path), f"File not found: {local_file_path}"

    quota_retry_deadline = time.time() + 2 * 60 * 60

    uploaded_file = await client.aio.files.upload(file=local_file_path)
    # 2) videos may be PROCESSING; poll until ACTIVE
    while uploaded_file.state != "ACTIVE":
        await asyncio.sleep(2)
        uploaded_file = await client.aio.files.get(name=uploaded_file.name)
    return uploaded_file.model_dump()


def extract_audio_from_video(video_path: str, outdir: str | None = None) -> str | None:
    if not video_path or not os.path.exists(video_path):
        logger.error(f"Video not found: {video_path!r}")
        return None

    if outdir is None:
        outdir = os.path.dirname(video_path)

    os.makedirs(outdir, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(video_path))[0]
    final_path = os.path.join(outdir, f"{base_name}_audio.wav")

    exe_path = get_ffmpeg_exe()

    tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    tmp_path = tmp.name
    tmp.close()

    cmd = [
        exe_path,
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        video_path,
        "-map",
        "0:a:0",
        "-vn",
        "-c:a",
        "pcm_s16le",
        "-ar",
        "16000",
        "-ac",
        "1",
        tmp_path,
    ]

    logger.debug("Running: %s", " ".join(cmd))

    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        logger.error("ffmpeg failed (code %s): %s", p.returncode, p.stderr)
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        return None

    if not os.path.exists(tmp_path) or os.path.getsize(tmp_path) == 0:
        logger.error("ffmpeg produced no audio output.")
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        return None

    shutil.move(tmp_path, final_path)
    return final_path


def get_video_width_height(video_path: str) -> tuple[int, int]:
    """return video width and height"""
    cap = cv2.VideoCapture(video_path)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    cap.release()
    return width, height


if __name__ == "__main__":
    EXAMPLE_VIDEO_PATH = "../../tests/fixtures/BigBuckBunny_320x180.mp4"
    print(extract_frames(EXAMPLE_VIDEO_PATH, 3, output_dir="../../tests/fixtures/"))
    print(get_chunks(EXAMPLE_VIDEO_PATH, 15))
    print(extract_audio_from_video(EXAMPLE_VIDEO_PATH, outdir="../../tests/fixtures/"))
    print(get_video_width_height(EXAMPLE_VIDEO_PATH))
