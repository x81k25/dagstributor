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
        "at-config",
        "environment",
        "transmission-config",
        "rear-diff-config",
        "reel-driver-config",
        "wst-config"
    ],
    "env_secrets": [
        "at-secrets",
        "transmission-secrets",
        "rear-diff-secrets",
        "wst-secrets"
    ],
    "job_spec_config": {
        "activeDeadlineSeconds": 3600,  # 1 hour timeout for ML workloads
        "backoffLimit": 1  # Allow 1 retry for transient failures
    },
}


reel_driver_training_feature_engineering_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": "ghcr.io/x81k25/reel-driver/training-feature-engineering:sha-72a25787f392f6db63f87504f21d5e6e18db2586",
    },
    name="reel_driver_training_feature_engineering_op"
)

reel_driver_model_training_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": "ghcr.io/x81k25/reel-driver/reel-driver-model-training:latest",
    },
    name="reel_driver_model_training_op"
)