# %%
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import spark

# Configure these to your environment
REQUIRED_TAGS: dict[str, str] = {
    "dev": "asafso",
    "env": "dev",
    "vertical": "deepmind_labeling",
}
w = WorkspaceClient()

# %% Find jobs by the tags

job_ids: set[int] = set()
for job in w.jobs.list():
    if job.creator_user_name != "asafso@activefence.com":
        continue
    tags: dict[str, str] = {}
    settings = job.settings
    if settings is not None and settings.tags is not None:
        try:
            tags = dict(settings.tags)
        except Exception:
            tags = {}
    if all(tags.get(k) == v for k, v in REQUIRED_TAGS.items()):
        if job.job_id is not None:
            job_ids.add(int(job.job_id))

# %% get active runs for the jobs

active_job_ids: set[int] = set()
for run in w.jobs.list_runs(active_only=True):
    if run.job_id is not None and int(run.job_id) in job_ids:
        active_job_ids.add(int(run.job_id))

# %%
if active_job_ids:
    print(f"Related jobs are active: {active_job_ids}. Aborting housekeeping.")
    exit(0)

# %% cleanup orphan tasks

TASKS_TODO = "af_delivery_dev.silver_deepmind_labeling.tasks_todo"
spark.sql(
    f"""
    select task_key, media_type, status, count(*) as num
    from {TASKS_TODO}
    group by task_key, media_type, status
    """
).show(truncate=False)

# %%
spark.sql(
    f"""
    select queue_id, count(*) as num
    from {TASKS_TODO}
    group by queue_id
    """
).show(truncate=False)

# %%
spark.sql(
    f"""
    delete from {TASKS_TODO}
    where status = 'pending'
    """
)
