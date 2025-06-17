"""Kubernetes container execution ops for automatic transmission pipeline."""

import os
from dagster import op, OpExecutionContext
from dagster_k8s import k8s_job_op


rss_ingest_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-rss-ingest:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    },
    name="rss_ingest_op"
)

collect_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-collect:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    },
    name="collect_op"
)

parse_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-parse:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    },
    name="parse_op"
)

file_filtration_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-file-filtration:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    },
    name="file_filtration_op"
)

metadata_collection_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-metadata-collection:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    },
    name="metadata_collection_op"
)

media_filtration_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-media-filtration:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    },
    name="media_filtration_op"
)

initiation_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-initiation:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    },
    name="initiation_op"
)

download_check_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-download-check:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    },
    name="download_check_op"
)

transfer_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-transfer:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    },
    name="transfer_op"
)

cleanup_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-cleanup:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    },
    name="cleanup_op"
)