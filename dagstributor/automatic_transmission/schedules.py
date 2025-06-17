"""Schedule definitions for automatic transmission pipeline."""

from dagster import schedule, DefaultScheduleStatus

from .jobs import (
    at_01_rss_ingest_job,
    at_02_collect_job,
    at_03_parse_job,
    at_04_file_filtration_job,
    at_05_metadata_collection_job,
    at_06_media_filtration_job,
    at_07_initiation_job,
    at_08_download_check_job,
    at_09_transfer_job,
    at_10_cleanup_job,
)


@schedule(
    job=at_01_rss_ingest_job,
    cron_schedule="0 * * * *",
    name="at_01_rss_ingest_schedule",
    default_status=DefaultScheduleStatus.RUNNING
)
def at_01_rss_ingest_schedule():
    """RSS Ingest runs at :00 minutes past each hour."""
    return {}


@schedule(
    job=at_02_collect_job,
    cron_schedule="6 * * * *",
    name="at_02_collect_schedule",
    default_status=DefaultScheduleStatus.RUNNING
)
def at_02_collect_schedule():
    """Collect runs at :06 minutes past each hour."""
    return {}


@schedule(
    job=at_03_parse_job,
    cron_schedule="12 * * * *",
    name="at_03_parse_schedule",
    default_status=DefaultScheduleStatus.RUNNING
)
def at_03_parse_schedule():
    """Parse runs at :12 minutes past each hour."""
    return {}


@schedule(
    job=at_04_file_filtration_job,
    cron_schedule="18 * * * *",
    name="at_04_file_filtration_schedule",
    default_status=DefaultScheduleStatus.RUNNING
)
def at_04_file_filtration_schedule():
    """File Filtration runs at :18 minutes past each hour."""
    return {}


@schedule(
    job=at_05_metadata_collection_job,
    cron_schedule="24 * * * *",
    name="at_05_metadata_collection_schedule",
    default_status=DefaultScheduleStatus.RUNNING
)
def at_05_metadata_collection_schedule():
    """Metadata Collection runs at :24 minutes past each hour."""
    return {}


@schedule(
    job=at_06_media_filtration_job,
    cron_schedule="30 * * * *",
    name="at_06_media_filtration_schedule",
    default_status=DefaultScheduleStatus.RUNNING
)
def at_06_media_filtration_schedule():
    """Media Filtration runs at :30 minutes past each hour."""
    return {}


@schedule(
    job=at_07_initiation_job,
    cron_schedule="36 * * * *",
    name="at_07_initiation_schedule",
    default_status=DefaultScheduleStatus.RUNNING
)
def at_07_initiation_schedule():
    """Initiation runs at :36 minutes past each hour."""
    return {}


@schedule(
    job=at_08_download_check_job,
    cron_schedule="42 * * * *",
    name="at_08_download_check_schedule",
    default_status=DefaultScheduleStatus.RUNNING
)
def at_08_download_check_schedule():
    """Download Check runs at :42 minutes past each hour."""
    return {}


@schedule(
    job=at_09_transfer_job,
    cron_schedule="48 * * * *",
    name="at_09_transfer_schedule",
    default_status=DefaultScheduleStatus.RUNNING
)
def at_09_transfer_schedule():
    """Transfer runs at :48 minutes past each hour."""
    return {}


@schedule(
    job=at_10_cleanup_job,
    cron_schedule="54 * * * *",
    name="at_10_cleanup_schedule",
    default_status=DefaultScheduleStatus.RUNNING
)
def at_10_cleanup_schedule():
    """Cleanup runs at :54 minutes past each hour."""
    return {}