"""Kubernetes container execution ops for reel-driver pipeline."""

import os
from dagster import op, OpExecutionContext
from dagster_k8s import k8s_job_op

# Image tag logic: prod environment uses 'main' tags, others use environment name
def get_image_tag():
    env = os.getenv('ENVIRONMENT', 'dev')
    return 'main' if env == 'prod' else env

# Global K8s job configuration for ML workloads
BASE_K8S_CONFIG = {
    "namespace": f"media-{os.getenv('ENVIRONMENT', 'dev')}",
    "image_pull_secrets": [{"name": "ghcr-pull-image-token"}],
    "env_config_maps": [
        "environment",
        "reel-driver-config",
        "reel-driver-training-config"
    ],
    "env_secrets": [
        "reel-driver-secrets",
        "reel-driver-training-secrets"
    ],
    "job_spec_config": {
        "activeDeadlineSeconds": 3600,  # 1 hour timeout for ML workloads
        "backoffLimit": 1  # Allow 1 retry for transient failures
    },
}


reel_driver_training_feature_engineering_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/reel-driver/reel-driver-feature-engineering:{get_image_tag()}",
    },
    name="reel_driver_training_feature_engineering_op"
)

reel_driver_model_training_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/reel-driver/reel-driver-model-training:{get_image_tag()}",
    },
    name="reel_driver_model_training_op"
)