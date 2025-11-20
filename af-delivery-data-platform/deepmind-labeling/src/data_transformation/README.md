## Triple-Table Framework

Developer-focused guide for the unified media labeling pipeline. At Silver, every task uses one pattern:

- `tasks_todo`: pending work and latest status
- `tasks_status`: append-only attempt/outcome events
- `tasks_results`: canonical, idempotent outputs

All tasks and modalities reuse the same pattern, keyed by `task_key` and `media_type`.

### Why

- Single pattern for all tasks and media types
- Deterministic IDs for idempotency and safe retries
- Append-only writes for concurrency and auditability
- Dependencies by IDs, not payload duplication

## Architecture

- Medallion: Landing (sources + prompts) → Silver (triple + runner) → Gold (consumable aggregates)
- Deterministic IDs derived from `(task_key, media_type, file_hash, prompt_id)`
- Registry-driven `silver_runner.py` implements three actions per task: `plan`, `process`, `consolidate`

### Execution

1) Plan: generate TODO by joining sources with prompts; exclude already-processed; emit `batch_id`s
2) Process: consume a `batch_id`, write attempt/final events, produce results
3) Consolidate: update TODO with latest state and upsert successful results

Examples (local debug):

```bash
python src/data_transformation/silver_runner.py plan --task video.extract_frames --limit 5
python src/data_transformation/silver_runner.py process --task video.extract_frames --batch_id <BATCH_ID>
python src/data_transformation/silver_runner.py consolidate --task video.extract_frames
```

Jobs in `resources/*.job.yml` wire `plan → process(for_each) → consolidate` via task values.

## Workflows

### Update prompts

- Prompt profiles are defined in JSON configuration files (referenced in `prompt_profiles_config.py`)
- Each prompt profile contains templates, model configurations, and versioning information
- When a prompt profile's hash changes, affected tasks will automatically reprocess existing media
- When add a new prompt, update `manage_prompt_profiles.py` accordingly.

After updating prompts, re-run `plan` for affected tasks to pick up the changes. 

### Create a new task

1. Add a spec under `src/data_transformation/tasks/` (e.g., `my_task_task.py`) with `get_*_spec()` that defines:
   - `task_key`, `media_type`
   - `processor(items) -> list[ProcessingResult]`
   - optionally `build_candidates_df(limit)` and `batch_strategy`
2. Register the task key so the runner can map `--task` to your spec
3. Run: `plan` → `process` (for_each fan-out) → `consolidate`
4. Gold: read from `tasks_results` for your `task_key`

## Improvements / TODO

- Testing: end-to-end pytest for `plan/process/consolidate` with Spark fixtures and Databricks smoke tests
- Centralized rate limiter: share quotas across tasks (see `llm_orchestrator/rate_limiter.py`)
- Housekeeping – remove orphans: detect/purge orphan TODO/status not linked to active results or stale batches
- Observability: throughput/latency/success metrics; structured errors
- Status compaction: periodic compaction of `tasks_status` and latest-state views
- Backfills: documented, parameterized backfill flows per `task_key`
