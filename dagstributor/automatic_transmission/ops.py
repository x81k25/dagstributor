"""Kubernetes container execution ops for automatic transmission pipeline."""

# Suppress BetaWarning before importing dagster modules
# Must be done before any dagster imports per:
# https://docs.dagster.io/api/api-lifecycle/filtering-api-lifecycle-warnings
import warnings
from dagster import BetaWarning
warnings.filterwarnings("ignore", category=BetaWarning)

import os
from dagster import op, OpExecutionContext
from dagster_k8s import k8s_job_op


def get_environment():
    """Get and validate ENVIRONMENT variable. Fails if not set."""
    env = os.environ.get('ENVIRONMENT')
    if not env:
        raise ValueError(
            "ENVIRONMENT variable is not set. "
            "This must be set to 'dev', 'stg', or 'prod'."
        )
    if env not in ['dev', 'stg', 'prod']:
        raise ValueError(
            f"Invalid ENVIRONMENT value: '{env}'. "
            f"Must be one of: 'dev', 'stg', 'prod'."
        )
    return env


# Image tag logic: prod environment uses 'main' tags, others use environment name
def get_image_tag():
    env = get_environment()
    return 'main' if env == 'prod' else env


# Global K8s job configuration - function to evaluate at runtime, not import time
def get_base_k8s_config():
    """Return base K8s config with namespace from ENVIRONMENT variable."""
    env = get_environment()
    return {
        "namespace": f"media-{env}",
        "image_pull_secrets": [{"name": "ghcr-pull-image-secret"}],
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
            "activeDeadlineSeconds": 60,  # 60 seconds global K8s operation timeout
            "backoffLimit": 0
        },
    }


at_01_rss_ingest_op = k8s_job_op.configured(
    {
        **get_base_k8s_config(),
        "image": f"ghcr.io/x81k25/automatic-transmission/at-01-rss-ingest:{get_image_tag()}",
        "container_config": {
            "name": "at-01-rss-ingest"
        }
    },
    name="at_01_rss_ingest_op"
)

at_02_collect_op = k8s_job_op.configured(
    {
        **get_base_k8s_config(),
        "image": f"ghcr.io/x81k25/automatic-transmission/at-02-collect:{get_image_tag()}",
        "container_config": {
            "name": "at-02-collect"
        }
    },
    name="at_02_collect_op"
)

at_03_parse_op = k8s_job_op.configured(
    {
        **get_base_k8s_config(),
        "image": f"ghcr.io/x81k25/automatic-transmission/at-03-parse:{get_image_tag()}",
        "container_config": {
            "name": "at-03-parse"
        }
    },
    name="at_03_parse_op"
)

at_04_file_filtration_op = k8s_job_op.configured(
    {
        **get_base_k8s_config(),
        "image": f"ghcr.io/x81k25/automatic-transmission/at-04-file-filtration:{get_image_tag()}",
        "container_config": {
            "name": "at-04-file-filtration"
        }
    },
    name="at_04_file_filtration_op"
)

at_05_metadata_collection_op = k8s_job_op.configured(
    {
        **get_base_k8s_config(),
        "image": f"ghcr.io/x81k25/automatic-transmission/at-05-metadata-collection:{get_image_tag()}",
        "container_config": {
            "name": "at-05-metadata-collection"
        }
    },
    name="at_05_metadata_collection_op"
)

at_06_media_filtration_op = k8s_job_op.configured(
    {
        **get_base_k8s_config(),
        "image": f"ghcr.io/x81k25/automatic-transmission/at-06-media-filtration:{get_image_tag()}",
        "container_config": {
            "name": "at-06-media-filtration"
        }
    },
    name="at_06_media_filtration_op"
)

at_07_initiation_op = k8s_job_op.configured(
    {
        **get_base_k8s_config(),
        "image": f"ghcr.io/x81k25/automatic-transmission/at-07-initiation:{get_image_tag()}",
        "container_config": {
            "name": "at-07-initiation"
        }
    },
    name="at_07_initiation_op"
)

at_08_download_check_op = k8s_job_op.configured(
    {
        **get_base_k8s_config(),
        "image": f"ghcr.io/x81k25/automatic-transmission/at-08-download-check:{get_image_tag()}",
        "container_config": {
            "name": "at-08-download-check"
        }
    },
    name="at_08_download_check_op"
)

# Get environment-specific paths from ConfigMap environment variables
DOWNLOAD_DIR = os.getenv('AT_DOWNLOAD_DIR')
MOVIE_DIR = os.getenv('AT_MOVIE_DIR')
TV_SHOW_DIR = os.getenv('AT_TV_SHOW_DIR')

at_09_transfer_op = k8s_job_op.configured(
    {
        **get_base_k8s_config(),
        "image": f"ghcr.io/x81k25/automatic-transmission/at-09-transfer:{get_image_tag()}",
        "job_spec_config": {
            "activeDeadlineSeconds": 600,  # 10 minutes for transfer operations
            "backoffLimit": 0
        },
        # Environment variables now available via at-config ConfigMap injection
        "container_config": {
            "name": "at-09-transfer",
            "volume_mounts": [
                {
                    "name": "download-volume",
                    "mount_path": DOWNLOAD_DIR,
                    "read_only": False
                },
                {
                    "name": "movie-volume",
                    "mount_path": MOVIE_DIR,
                    "read_only": False
                },
                {
                    "name": "tv-volume",
                    "mount_path": TV_SHOW_DIR,
                    "read_only": False
                }
            ]
        },
        "pod_spec_config": {
            "volumes": [
                {
                    "name": "download-volume",
                    "host_path": {
                        "path": DOWNLOAD_DIR,
                        "type": "DirectoryOrCreate"
                    }
                },
                {
                    "name": "movie-volume",
                    "host_path": {
                        "path": MOVIE_DIR,
                        "type": "DirectoryOrCreate"
                    }
                },
                {
                    "name": "tv-volume",
                    "host_path": {
                        "path": TV_SHOW_DIR,
                        "type": "DirectoryOrCreate"
                    }
                }
            ]
        }
    },
    name="at_09_transfer_op"
)

at_10_cleanup_op = k8s_job_op.configured(
    {
        **get_base_k8s_config(),
        "image": f"ghcr.io/x81k25/automatic-transmission/at-10-cleanup:{get_image_tag()}",
        "container_config": {
            "name": "at-10-cleanup"
        }
    },
    name="at_10_cleanup_op"
)