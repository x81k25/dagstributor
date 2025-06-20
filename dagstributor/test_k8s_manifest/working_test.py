"""Test job that follows the exact pattern of working jobs"""

from dagster import job
from dagster_k8s import k8s_job_op

# Create a k8s op exactly like the working ones
test_k8s_working_op = k8s_job_op.configured({
    "image": "busybox:latest",
    "command": ["/bin/sh", "-c", "echo 'SUCCESS: K8s manifest properly applied!' && date"],
    "namespace": "media-dev",
    "image_pull_policy": "IfNotPresent",
    "env_vars": ["DAGSTER_RUN_JOB_NAME"],
    "container_config": {
        "resources": {
            "requests": {"cpu": "100m", "memory": "128Mi"},
            "limits": {"cpu": "500m", "memory": "512Mi"}
        }
    }
}, name="test_k8s_working_op")

@job
def test_k8s_working_job():
    """Test job that can be triggered manually via UI"""
    test_k8s_working_op()