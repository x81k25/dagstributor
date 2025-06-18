"""Job definitions for automatic transmission pipeline."""

from dagster import job

from .ops import (
    at_01_rss_ingest_op,
    at_02_collect_op,
    at_03_parse_op,
    at_04_file_filtration_op,
    at_05_metadata_collection_op,
    at_06_media_filtration_op,
    at_07_initiation_op,
    at_08_download_check_op,
    at_09_transfer_op,
    at_10_cleanup_op,
)


@job(tags={"dagster/max_runtime": "30"})
def at_01_rss_ingest_job():
    """RSS Ingest job - runs at :00 minutes."""
    at_01_rss_ingest_op()


@job
def at_02_collect_job():
    """Collect job - runs at :06 minutes."""
    at_02_collect_op()


@job
def at_03_parse_job():
    """Parse job - runs at :12 minutes."""
    at_03_parse_op()


@job
def at_04_file_filtration_job():
    """File Filtration job - runs at :18 minutes."""
    at_04_file_filtration_op()


@job
def at_05_metadata_collection_job():
    """Metadata Collection job - runs at :24 minutes."""
    at_05_metadata_collection_op()


@job
def at_06_media_filtration_job():
    """Media Filtration job - runs at :30 minutes."""
    at_06_media_filtration_op()


@job
def at_07_initiation_job():
    """Initiation job - runs at :36 minutes."""
    at_07_initiation_op()


@job
def at_08_download_check_job():
    """Download Check job - runs at :42 minutes."""
    at_08_download_check_op()


@job
def at_09_transfer_job():
    """Transfer job - runs at :48 minutes."""
    at_09_transfer_op()


@job
def at_10_cleanup_job():
    """Cleanup job - runs at :54 minutes."""
    at_10_cleanup_op()