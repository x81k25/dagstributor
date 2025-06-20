from dagster import job, op, schedule, DefaultScheduleStatus
from dagster_k8s import k8s_job_op
import yaml

# Method 5: Create job with immediate schedule (runs at next minute)
@op
def create_manifest_op(context):
    """Create a K8s manifest programmatically"""
    manifest = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "test-manifest-immediate",
            "namespace": "media-dev"
        },
        "spec": {
            "template": {
                "spec": {
                    "containers": [{
                        "name": "test-container",
                        "image": "busybox:latest",
                        "command": ["/bin/sh", "-c", "echo 'Testing immediate manifest creation' && date"]
                    }],
                    "restartPolicy": "Never"
                }
            }
        }
    }
    context.log.info(f"Created manifest: {yaml.dump(manifest)}")
    return manifest

# Use k8s_job_op with minimal config
immediate_k8s_op = k8s_job_op.configured({
    "image": "busybox:latest",
    "command": ["/bin/sh", "-c", "echo 'Method 5: Immediate test' && date && echo 'COMPLETED'"],
    "namespace": "media-dev",
    "name": "test-immediate",
    "image_pull_policy": "IfNotPresent"
}, name="immediate_k8s_op")

@job
def test_immediate_job():
    manifest = create_manifest_op()
    immediate_k8s_op()

# Schedule that runs every minute for testing
@schedule(
    cron_schedule="* * * * *",
    job=test_immediate_job,
    default_status=DefaultScheduleStatus.RUNNING
)
def test_immediate_schedule():
    return {}