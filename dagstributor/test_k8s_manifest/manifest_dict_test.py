from dagster import job, op, In, Out
from dagster_k8s import k8s_job_op
import json

# Method 4: Using k8s_job_op with full manifest dict
manifest_dict_op = k8s_job_op.configured({
    "image": "busybox:latest",
    "command": ["/bin/sh"],
    "args": ["-c", "echo 'Method 4: Testing manifest dict' && env | grep DAGSTER"],
    "namespace": "media-dev",
    "name": "test-manifest-dict",
    "image_pull_policy": "IfNotPresent",
    "env_vars": ["DAGSTER_RUN_JOB_NAME"],
    "container_config": {
        "resources": {
            "requests": {
                "cpu": "100m",
                "memory": "128Mi"
            },
            "limits": {
                "cpu": "200m",
                "memory": "256Mi"
            }
        }
    },
    "pod_spec_config": {
        "restart_policy": "Never"
    }
}, name="manifest_dict_op")

# Create a sensor that auto-triggers this job
from dagster import sensor, RunRequest, SkipReason
import time

@job
def test_manifest_dict_job():
    manifest_dict_op()

@sensor(job=test_manifest_dict_job, minimum_interval_seconds=300)
def test_manifest_sensor(context):
    # Check if we should run (only run once per hour at :45)
    current_minute = time.localtime().tm_min
    if current_minute == 45:
        return RunRequest(run_key=f"test_manifest_{int(time.time())}")
    return SkipReason("Not time to run yet")