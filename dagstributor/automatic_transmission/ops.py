"""Kubernetes container execution ops for automatic transmission pipeline."""

import os
from dagster import op, OpExecutionContext
from dagster_k8s import k8s_job_op

# Image tag logic: prod environment uses 'main' tags, others use environment name
def get_image_tag():
    env = os.getenv('ENVIRONMENT', 'dev')
    return 'main' if env == 'prod' else env

# Global K8s job configuration
BASE_K8S_CONFIG = {
    "namespace": f"media-{os.getenv('ENVIRONMENT', 'dev')}",
    "image_pull_secrets": [{"name": "ghcr-pull-image-token"}],
    "env_config_maps": ["at-config", "environment"],
    "env_secrets": ["at-sensitive"],
    "job_spec_config": {
        "activeDeadlineSeconds": 60,  # 60 seconds global K8s operation timeout
        "backoffLimit": 0
    },
}


at_01_rss_ingest_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-01-rss-ingest:{get_image_tag()}",
    },
    name="at_01_rss_ingest_op"
)

at_02_collect_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-02-collect:{get_image_tag()}",
    },
    name="at_02_collect_op"
)

at_03_parse_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-03-parse:{get_image_tag()}",
    },
    name="at_03_parse_op"
)

at_04_file_filtration_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-04-file-filtration:{get_image_tag()}",
    },
    name="at_04_file_filtration_op"
)

at_05_metadata_collection_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-05-metadata-collection:{get_image_tag()}",
    },
    name="at_05_metadata_collection_op"
)

at_06_media_filtration_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-06-media-filtration:{get_image_tag()}",
    },
    name="at_06_media_filtration_op"
)

at_07_initiation_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-07-initiation:{get_image_tag()}",
    },
    name="at_07_initiation_op"
)

at_08_download_check_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-08-download-check:{get_image_tag()}",
    },
    name="at_08_download_check_op"
)

# Get environment-specific paths from ConfigMap environment variables
DOWNLOAD_DIR = os.getenv('DOWNLOAD_DIR')
MOVIE_DIR = os.getenv('MOVIE_DIR')
TV_SHOW_DIR = os.getenv('TV_SHOW_DIR')

at_09_transfer_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-09-transfer:{get_image_tag()}",
        # Environment variables now available via at-config ConfigMap injection
        "container_config": {
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
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-10-cleanup:{get_image_tag()}",
    },
    name="at_10_cleanup_op"
)