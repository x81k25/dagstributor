"""Schedule definitions for automatic transmission pipeline."""

from dagster import schedule

from .jobs import (
    rss_ingest_job,
    collect_job,
    parse_job,
    file_filtration_job,
    metadata_collection_job,
    media_filtration_job,
    initiation_job,
    download_check_job,
    transfer_job,
    cleanup_job,
)


@schedule(
    job=rss_ingest_job,
    cron_schedule="0 * * * *",
    name="rss_ingest_schedule"
)
def rss_ingest_schedule():
    """RSS Ingest runs at :00 minutes past each hour."""
    return {}


@schedule(
    job=collect_job,
    cron_schedule="6 * * * *",
    name="collect_schedule"
)
def collect_schedule():
    """Collect runs at :06 minutes past each hour."""
    return {}


@schedule(
    job=parse_job,
    cron_schedule="12 * * * *",
    name="parse_schedule"
)
def parse_schedule():
    """Parse runs at :12 minutes past each hour."""
    return {}


@schedule(
    job=file_filtration_job,
    cron_schedule="18 * * * *",
    name="file_filtration_schedule"
)
def file_filtration_schedule():
    """File Filtration runs at :18 minutes past each hour."""
    return {}


@schedule(
    job=metadata_collection_job,
    cron_schedule="24 * * * *",
    name="metadata_collection_schedule"
)
def metadata_collection_schedule():
    """Metadata Collection runs at :24 minutes past each hour."""
    return {}


@schedule(
    job=media_filtration_job,
    cron_schedule="30 * * * *",
    name="media_filtration_schedule"
)
def media_filtration_schedule():
    """Media Filtration runs at :30 minutes past each hour."""
    return {}


@schedule(
    job=initiation_job,
    cron_schedule="36 * * * *",
    name="initiation_schedule"
)
def initiation_schedule():
    """Initiation runs at :36 minutes past each hour."""
    return {}


@schedule(
    job=download_check_job,
    cron_schedule="42 * * * *",
    name="download_check_schedule"
)
def download_check_schedule():
    """Download Check runs at :42 minutes past each hour."""
    return {}


@schedule(
    job=transfer_job,
    cron_schedule="48 * * * *",
    name="transfer_schedule"
)
def transfer_schedule():
    """Transfer runs at :48 minutes past each hour."""
    return {}


@schedule(
    job=cleanup_job,
    cron_schedule="54 * * * *",
    name="cleanup_schedule"
)
def cleanup_schedule():
    """Cleanup runs at :54 minutes past each hour."""
    return {}