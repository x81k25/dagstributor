"""Kubernetes container execution ops for automatic transmission pipeline."""

import os
from dagster import op, OpExecutionContext
from dagster_k8s import k8s_job_op


rss_ingest_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-01-rss-ingest:{os.getenv('ENVIRONMENT', 'dev')}",
        "namespace": f"media-{os.getenv('ENVIRONMENT', 'dev')}",
        "image_pull_secrets": [{"name": "ghcr-pull-image-token"}],
    },
    name="rss_ingest_op"
)

collect_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-02-collect:{os.getenv('ENVIRONMENT', 'dev')}",
        "namespace": f"media-{os.getenv('ENVIRONMENT', 'dev')}",
        "image_pull_secrets": [{"name": "ghcr-pull-image-token"}],
    },
    name="collect_op"
)

parse_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-03-parse:{os.getenv('ENVIRONMENT', 'dev')}",
        "namespace": f"media-{os.getenv('ENVIRONMENT', 'dev')}",
        "image_pull_secrets": [{"name": "ghcr-pull-image-token"}],
    },
    name="parse_op"
)

file_filtration_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-04-file-filtration:{os.getenv('ENVIRONMENT', 'dev')}",
        "namespace": f"media-{os.getenv('ENVIRONMENT', 'dev')}",
        "image_pull_secrets": [{"name": "ghcr-pull-image-token"}],
    },
    name="file_filtration_op"
)

metadata_collection_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-05-metadata-collection:{os.getenv('ENVIRONMENT', 'dev')}",
        "namespace": f"media-{os.getenv('ENVIRONMENT', 'dev')}",
        "image_pull_secrets": [{"name": "ghcr-pull-image-token"}],
    },
    name="metadata_collection_op"
)

media_filtration_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-06-media-filtration:{os.getenv('ENVIRONMENT', 'dev')}",
        "namespace": f"media-{os.getenv('ENVIRONMENT', 'dev')}",
        "image_pull_secrets": [{"name": "ghcr-pull-image-token"}],
    },
    name="media_filtration_op"
)

initiation_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-07-initiation:{os.getenv('ENVIRONMENT', 'dev')}",
        "namespace": f"media-{os.getenv('ENVIRONMENT', 'dev')}",
        "image_pull_secrets": [{"name": "ghcr-pull-image-token"}],
    },
    name="initiation_op"
)

download_check_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-08-download-check:{os.getenv('ENVIRONMENT', 'dev')}",
        "namespace": f"media-{os.getenv('ENVIRONMENT', 'dev')}",
        "image_pull_secrets": [{"name": "ghcr-pull-image-token"}],
    },
    name="download_check_op"
)

transfer_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-09-transfer:{os.getenv('ENVIRONMENT', 'dev')}",
        "namespace": f"media-{os.getenv('ENVIRONMENT', 'dev')}",
        "image_pull_secrets": [{"name": "ghcr-pull-image-token"}],
    },
    name="transfer_op"
)

cleanup_op = k8s_job_op.configured(
    {
        "image": f"ghcr.io/x81k25/automatic-transmission/at-10-cleanup:{os.getenv('ENVIRONMENT', 'dev')}",
        "namespace": f"media-{os.getenv('ENVIRONMENT', 'dev')}",
        "image_pull_secrets": [{"name": "ghcr-pull-image-token"}],
    },
    name="cleanup_op"
)