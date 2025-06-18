"""Kubernetes container execution ops for automatic transmission pipeline."""

import os
from dagster import op, OpExecutionContext
from dagster_k8s import k8s_job_op

# Global K8s job configuration
BASE_K8S_CONFIG = {
    "namespace": f"media-{os.getenv('ENVIRONMENT', 'dev')}",
    "image_pull_secrets": [{"name": "ghcr-pull-image-token"}],
    "env_config_maps": ["at-config", "environment"],
    "env_secrets": ["at-sensitive"],
    "job_spec_config": {
        "activeDeadlineSeconds": 300,  # 5 minutes global K8s operation timeout
        "backoffLimit": 0
    },
}


at_01_rss_ingest_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-01-rss-ingest:{os.getenv('ENVIRONMENT', 'dev')}",
    },
    name="at_01_rss_ingest_op"
)

at_02_collect_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-02-collect:{os.getenv('ENVIRONMENT', 'dev')}",
    },
    name="at_02_collect_op"
)

at_03_parse_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-03-parse:{os.getenv('ENVIRONMENT', 'dev')}",
    },
    name="at_03_parse_op"
)

at_04_file_filtration_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-04-file-filtration:{os.getenv('ENVIRONMENT', 'dev')}",
    },
    name="at_04_file_filtration_op"
)

at_05_metadata_collection_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-05-metadata-collection:{os.getenv('ENVIRONMENT', 'dev')}",
    },
    name="at_05_metadata_collection_op"
)

at_06_media_filtration_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-06-media-filtration:{os.getenv('ENVIRONMENT', 'dev')}",
    },
    name="at_06_media_filtration_op"
)

at_07_initiation_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-07-initiation:{os.getenv('ENVIRONMENT', 'dev')}",
    },
    name="at_07_initiation_op"
)

at_08_download_check_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-08-download-check:{os.getenv('ENVIRONMENT', 'dev')}",
    },
    name="at_08_download_check_op"
)

at_09_transfer_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-09-transfer:{os.getenv('ENVIRONMENT', 'dev')}",
    },
    name="at_09_transfer_op"
)

at_10_cleanup_op = k8s_job_op.configured(
    {
        **BASE_K8S_CONFIG,
        "image": f"ghcr.io/x81k25/automatic-transmission/at-10-cleanup:{os.getenv('ENVIRONMENT', 'dev')}",
    },
    name="at_10_cleanup_op"
)