"""Job definitions for automatic transmission pipeline."""

from dagster import job

from .ops import (
    rss_ingest_op,
    collect_op,
    parse_op,
    file_filtration_op,
    metadata_collection_op,
    media_filtration_op,
    initiation_op,
    download_check_op,
    transfer_op,
    cleanup_op,
)


@job
def rss_ingest_job():
    """RSS Ingest job - runs at :00 minutes."""
    rss_ingest_op()


@job
def collect_job():
    """Collect job - runs at :06 minutes."""
    collect_op()


@job
def parse_job():
    """Parse job - runs at :12 minutes."""
    parse_op()


@job
def file_filtration_job():
    """File Filtration job - runs at :18 minutes."""
    file_filtration_op()


@job
def metadata_collection_job():
    """Metadata Collection job - runs at :24 minutes."""
    metadata_collection_op()


@job
def media_filtration_job():
    """Media Filtration job - runs at :30 minutes."""
    media_filtration_op()


@job
def initiation_job():
    """Initiation job - runs at :36 minutes."""
    initiation_op()


@job
def download_check_job():
    """Download Check job - runs at :42 minutes."""
    download_check_op()


@job
def transfer_job():
    """Transfer job - runs at :48 minutes."""
    transfer_op()


@job
def cleanup_job():
    """Cleanup job - runs at :54 minutes."""
    cleanup_op()