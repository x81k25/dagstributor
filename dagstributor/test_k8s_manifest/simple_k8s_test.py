"""Simple K8s job test following automatic_transmission pattern"""

import os
from dagster import job
from dagster_k8s import k8s_job_op

# Simple test op that follows the existing pattern exactly
test_k8s_simple = k8s_job_op.configured(
    {
        "namespace": f"media-{os.getenv('ENVIRONMENT', 'dev')}",
        "image": "busybox:latest",
        "command": ["sh", "-c", "echo 'Simple K8s test running!' && date && sleep 5 && echo 'Test completed!'"],
        "image_pull_secrets": [{"name": "ghcr-pull-image-token"}],
        "job_spec_config": {
            "ttl_seconds_after_finished": 60,
            "backoff_limit": 0
        },
        "load_incluster_config": True,
    },
    name="test_k8s_simple"
)

@job
def test_k8s_simple_job():
    """Simple test job for k8s manifest application"""
    test_k8s_simple()