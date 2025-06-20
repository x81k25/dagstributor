"""Test K8s manifest application methods"""

from dagster import op, job, In, Out, Field, String
from dagster_k8s import k8s_job_op
import os
import json
import yaml
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# Method 1: Simple inline manifest using k8s_job_op
test_manifest_method1 = k8s_job_op.configured(
    config={
        "image": "busybox:latest",
        "command": ["echo", "Method 1: Testing manifest application"],
        "namespace": f"media-{os.getenv('ENVIRONMENT', 'dev')}",
        "load_incluster_config": True,
        "image_pull_secrets": [{"name": "ghcr-pull-image-token"}],
        "job_spec_config": {
            "ttl_seconds_after_finished": 60,
            "backoff_limit": 0
        }
    },
    name="test_manifest_method1"
)

# Method 2: Using raw manifest with k8s Python client
@op(
    config_schema={
        "manifest": Field(String, description="Raw K8s manifest as YAML string"),
        "namespace": Field(String, default_value=f"media-{os.getenv('ENVIRONMENT', 'dev')}")
    }
)
def test_manifest_method2(context):
    """Apply a raw K8s manifest using the Python client"""
    
    # Load kubernetes config
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()
    
    # Parse manifest
    manifest_str = context.op_config["manifest"]
    manifest = yaml.safe_load(manifest_str)
    
    # Apply manifest based on kind
    api_client = client.ApiClient()
    
    if manifest["kind"] == "Job":
        batch_v1 = client.BatchV1Api(api_client)
        namespace = context.op_config["namespace"]
        
        try:
            response = batch_v1.create_namespaced_job(
                namespace=namespace,
                body=manifest
            )
            context.log.info(f"Job created: {response.metadata.name}")
            return response.metadata.name
        except ApiException as e:
            context.log.error(f"Failed to create job: {e}")
            raise

# Method 3: Using k8s_job_op with container_config
test_manifest_method3 = k8s_job_op.configured(
    config={
        "namespace": f"media-{os.getenv('ENVIRONMENT', 'dev')}",
        "load_incluster_config": True,
        "container_config": {
            "image": "busybox:latest",
            "command": ["sh", "-c"],
            "args": ["echo 'Method 3: Testing with container_config' && sleep 10"],
            "env": [
                {"name": "TEST_ENV", "value": "method3"}
            ]
        },
        "pod_spec_config": {
            "image_pull_secrets": [{"name": "ghcr-pull-image-token"}]
        },
        "job_spec_config": {
            "ttl_seconds_after_finished": 60,
            "backoff_limit": 0
        }
    },
    name="test_manifest_method3"
)

# Test job combining all methods
@job
def test_k8s_manifest_job():
    test_manifest_method1()
    test_manifest_method3()