"""Schedule definitions for automatic transmission pipeline."""

from dagster import schedule, DefaultScheduleStatus
from .config_loader import CONFIG

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
    cron_schedule=CONFIG["schedules"]["at_01_rss_ingest"]["cron_schedule"],
    name="at_01_rss_ingest_schedule",
    default_status=getattr(DefaultScheduleStatus, CONFIG["schedules"]["at_01_rss_ingest"]["default_status"])
)
def at_01_rss_ingest_schedule():
    """RSS Ingest runs at :00 minutes past each hour."""
    return {}


@schedule(
    job=at_02_collect_job,
    cron_schedule=CONFIG["schedules"]["at_02_collect"]["cron_schedule"],
    name="at_02_collect_schedule",
    default_status=getattr(DefaultScheduleStatus, CONFIG["schedules"]["at_02_collect"]["default_status"])
)
def at_02_collect_schedule():
    """Collect runs at :06 minutes past each hour."""
    return {}


@schedule(
    job=at_03_parse_job,
    cron_schedule=CONFIG["schedules"]["at_03_parse"]["cron_schedule"],
    name="at_03_parse_schedule",
    default_status=getattr(DefaultScheduleStatus, CONFIG["schedules"]["at_03_parse"]["default_status"])
)
def at_03_parse_schedule():
    """Parse runs at :12 minutes past each hour."""
    return {}


@schedule(
    job=at_04_file_filtration_job,
    cron_schedule=CONFIG["schedules"]["at_04_file_filtration"]["cron_schedule"],
    name="at_04_file_filtration_schedule",
    default_status=getattr(DefaultScheduleStatus, CONFIG["schedules"]["at_04_file_filtration"]["default_status"])
)
def at_04_file_filtration_schedule():
    """File Filtration runs at :18 minutes past each hour."""
    return {}


@schedule(
    job=at_05_metadata_collection_job,
    cron_schedule=CONFIG["schedules"]["at_05_metadata_collection"]["cron_schedule"],
    name="at_05_metadata_collection_schedule",
    default_status=getattr(DefaultScheduleStatus, CONFIG["schedules"]["at_05_metadata_collection"]["default_status"])
)
def at_05_metadata_collection_schedule():
    """Metadata Collection runs at :24 minutes past each hour."""
    return {}


@schedule(
    job=at_06_media_filtration_job,
    cron_schedule=CONFIG["schedules"]["at_06_media_filtration"]["cron_schedule"],
    name="at_06_media_filtration_schedule",
    default_status=getattr(DefaultScheduleStatus, CONFIG["schedules"]["at_06_media_filtration"]["default_status"])
)
def at_06_media_filtration_schedule():
    """Media Filtration runs at :30 minutes past each hour."""
    return {}


@schedule(
    job=at_07_initiation_job,
    cron_schedule=CONFIG["schedules"]["at_07_initiation"]["cron_schedule"],
    name="at_07_initiation_schedule",
    default_status=getattr(DefaultScheduleStatus, CONFIG["schedules"]["at_07_initiation"]["default_status"])
)
def at_07_initiation_schedule():
    """Initiation runs at :36 minutes past each hour."""
    return {}


@schedule(
    job=at_08_download_check_job,
    cron_schedule=CONFIG["schedules"]["at_08_download_check"]["cron_schedule"],
    name="at_08_download_check_schedule",
    default_status=getattr(DefaultScheduleStatus, CONFIG["schedules"]["at_08_download_check"]["default_status"])
)
def at_08_download_check_schedule():
    """Download Check runs at :42 minutes past each hour."""
    return {}


@schedule(
    job=at_09_transfer_job,
    cron_schedule=CONFIG["schedules"]["at_09_transfer"]["cron_schedule"],
    name="at_09_transfer_schedule",
    default_status=getattr(DefaultScheduleStatus, CONFIG["schedules"]["at_09_transfer"]["default_status"])
)
def at_09_transfer_schedule():
    """Transfer runs at :48 minutes past each hour."""
    return {}


@schedule(
    job=at_10_cleanup_job,
    cron_schedule=CONFIG["schedules"]["at_10_cleanup"]["cron_schedule"],
    name="at_10_cleanup_schedule",
    default_status=getattr(DefaultScheduleStatus, CONFIG["schedules"]["at_10_cleanup"]["default_status"])
)
def at_10_cleanup_schedule():
    """Cleanup runs at :54 minutes past each hour."""
    return {}