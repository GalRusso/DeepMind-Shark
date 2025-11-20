import argparse

from constants import TaskKey
from databricks.sdk.runtime import dbutils
from task_framework import (
    TaskSpec,
    build_and_enqueue_todos,
    consolidate_task,
    process_batch,
)

# Image tasks
from tasks.image_abuse_profiling_task import get_image_abuse_spec
from tasks.image_des_tag_task import get_image_des_tag_spec
from tasks.image_embedding_task import get_image_embedding_spec
from tasks.video_audio_transcribe_task import get_video_audio_transcribe_spec
from tasks.video_audio_translate_task import get_video_audio_translate_spec
from tasks.video_des_tag_task import get_video_des_tag_spec
from tasks.video_embedding_task import get_video_embedding_spec
from tasks.video_extract_audio_task import get_video_extract_audio_spec

# Video tasks
from tasks.video_extract_frames_task import get_video_extract_frames_spec
from tasks.video_frame_abuse_consolidate_task import get_video_frame_abuse_consolidate_spec
from tasks.video_frame_abuse_task import get_video_frame_abuse_spec


def _get_spec(task_key: TaskKey) -> TaskSpec:
    match task_key:
        case TaskKey.image_des_tag:
            return get_image_des_tag_spec()
        case TaskKey.image_abuse:
            return get_image_abuse_spec()
        case TaskKey.image_embedding:
            return get_image_embedding_spec()
        case TaskKey.video_extract_frames:
            return get_video_extract_frames_spec()
        case TaskKey.video_des_tag:
            return get_video_des_tag_spec()
        case TaskKey.video_embedding:
            return get_video_embedding_spec()
        case TaskKey.video_frame_abuse:
            return get_video_frame_abuse_spec()
        case TaskKey.video_frame_abuse_consolidate:
            return get_video_frame_abuse_consolidate_spec()
        case TaskKey.video_extract_audio:
            return get_video_extract_audio_spec()
        case TaskKey.video_audio_transcribe:
            return get_video_audio_transcribe_spec()
        case TaskKey.video_audio_translate:
            return get_video_audio_translate_spec()
    raise SystemExit(f"Task not registered: {task_key.value}")


def _cmd_plan(args: argparse.Namespace) -> None:
    task_key = TaskKey(args.task)
    spec = _get_spec(task_key)
    if args.limit is None or args.limit == "":
        limit = None
    else:
        limit = int(args.limit)
    batch_ids = build_and_enqueue_todos(spec, limit=limit)
    dbutils.jobs.taskValues.set("pending_batch_ids", batch_ids)
    print(f"Queued {len(batch_ids)} batches for {task_key.value}.")


def _cmd_process(args: argparse.Namespace) -> None:
    task_key = TaskKey(args.task)
    spec = _get_spec(task_key)
    if not args.batch_id:
        raise SystemExit("--batch_id is required for process action")
    process_batch(args.batch_id, spec)
    print(f"Processed batch {args.batch_id} for {task_key.value}.")


def _cmd_consolidate(args: argparse.Namespace) -> None:
    task_key = TaskKey(args.task)
    spec = _get_spec(task_key)
    consolidate_task(spec)
    print(f"Consolidated results for {task_key.value}.")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="silver_runner")
    sub = parser.add_subparsers(dest="action", required=True)

    # plan
    p_plan = sub.add_parser("plan", help="Enqueue TODOs for a task")
    p_plan.add_argument("--task", required=True, help="Task key, e.g., video.extract_frames")
    p_plan.add_argument("--limit", required=True, help="Limit candidate items (int)")
    p_plan.set_defaults(func=_cmd_plan)

    # process
    p_process = sub.add_parser("process", help="Process a batch for a task")
    p_process.add_argument("--task", required=True, help="Task key, e.g., video.extract_frames")
    p_process.add_argument("--batch_id", required=True, help="Batch identifier to process")
    p_process.set_defaults(func=_cmd_process)

    # consolidate
    p_con = sub.add_parser("consolidate", help="Consolidate results for a task")
    p_con.add_argument("--task", required=True, help="Task key, e.g., video.extract_frames")
    p_con.set_defaults(func=_cmd_consolidate)

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
