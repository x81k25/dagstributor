"""Kubernetes container execution ops for automatic transmission pipeline."""

import os
from dagster import op, OpExecutionContext
from dagster_k8s import k8s_job_op


@k8s_job_op(
    container_config={
        "image": f"ghcr.io/x81k25/automatic-transmission/at-rss-ingest:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    }
)
def rss_ingest_op(context: OpExecutionContext):
    """RSS Ingest container operation."""
    context.log.info("Running RSS Ingest container")


@k8s_job_op(
    container_config={
        "image": f"ghcr.io/x81k25/automatic-transmission/at-collect:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    }
)
def collect_op(context: OpExecutionContext):
    """Collect container operation."""
    context.log.info("Running Collect container")


@k8s_job_op(
    container_config={
        "image": f"ghcr.io/x81k25/automatic-transmission/at-parse:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    }
)
def parse_op(context: OpExecutionContext):
    """Parse container operation."""
    context.log.info("Running Parse container")


@k8s_job_op(
    container_config={
        "image": f"ghcr.io/x81k25/automatic-transmission/at-file-filtration:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    }
)
def file_filtration_op(context: OpExecutionContext):
    """File Filtration container operation."""
    context.log.info("Running File Filtration container")


@k8s_job_op(
    container_config={
        "image": f"ghcr.io/x81k25/automatic-transmission/at-metadata-collection:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    }
)
def metadata_collection_op(context: OpExecutionContext):
    """Metadata Collection container operation."""
    context.log.info("Running Metadata Collection container")


@k8s_job_op(
    container_config={
        "image": f"ghcr.io/x81k25/automatic-transmission/at-media-filtration:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    }
)
def media_filtration_op(context: OpExecutionContext):
    """Media Filtration container operation."""
    context.log.info("Running Media Filtration container")


@k8s_job_op(
    container_config={
        "image": f"ghcr.io/x81k25/automatic-transmission/at-initiation:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    }
)
def initiation_op(context: OpExecutionContext):
    """Initiation container operation."""
    context.log.info("Running Initiation container")


@k8s_job_op(
    container_config={
        "image": f"ghcr.io/x81k25/automatic-transmission/at-download-check:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    }
)
def download_check_op(context: OpExecutionContext):
    """Download Check container operation."""
    context.log.info("Running Download Check container")


@k8s_job_op(
    container_config={
        "image": f"ghcr.io/x81k25/automatic-transmission/at-transfer:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    }
)
def transfer_op(context: OpExecutionContext):
    """Transfer container operation."""
    context.log.info("Running Transfer container")


@k8s_job_op(
    container_config={
        "image": f"ghcr.io/x81k25/automatic-transmission/at-cleanup:{os.getenv('ENVIRONMENT', 'latest')}",
        "env_config_maps": ["dagster-pipeline-env"],
    }
)
def cleanup_op(context: OpExecutionContext):
    """Cleanup container operation."""
    context.log.info("Running Cleanup container")