from dagster import job, schedule, DefaultScheduleStatus
from dagster_k8s import k8s_job_op

# Method 3: Create a scheduled job that runs automatically
test_k8s_scheduled_op = k8s_job_op.configured({
    "image": "busybox:latest",
    "command": ["/bin/sh"],
    "args": ["-c", "echo 'Method 3: Scheduled K8s test running at' $(date)"],
    "namespace": "media-dev",
    "name": "test-k8s-scheduled",
    "image_pull_policy": "IfNotPresent",
    "env_vars": ["DAGSTER_RUN_JOB_NAME"]
}, name="test_k8s_scheduled_op")

@job
def test_k8s_scheduled_job():
    test_k8s_scheduled_op()

# Schedule to run every 5 minutes for testing
@schedule(
    cron_schedule="*/5 * * * *",
    job=test_k8s_scheduled_job,
    default_status=DefaultScheduleStatus.RUNNING
)
def test_k8s_schedule():
    return {}