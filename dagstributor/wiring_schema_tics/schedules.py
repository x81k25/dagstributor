"""Schedule definitions for wiring schema tics pipeline."""

from dagster import schedule, DefaultScheduleStatus
from ..automatic_transmission.config_loader import CONFIG

from .jobs import wst_atp_bak_job, wst_atp_sync_media_to_training_job


@schedule(
    job=wst_atp_bak_job,
    cron_schedule=CONFIG["schedules"]["wst_atp_bak"]["cron_schedule"],
    name="wst_atp_bak_schedule",
    default_status=getattr(DefaultScheduleStatus, CONFIG["schedules"]["wst_atp_bak"]["default_status"])
)
def wst_atp_bak_schedule():
    """ATP Backup runs according to environment-specific schedule."""
    return {}


@schedule(
    job=wst_atp_sync_media_to_training_job,
    cron_schedule=CONFIG["schedules"]["wst_atp_sync_media_to_training"]["cron_schedule"],
    name="wst_atp_sync_media_to_training_schedule",
    default_status=getattr(DefaultScheduleStatus, CONFIG["schedules"]["wst_atp_sync_media_to_training"]["default_status"])
)
def wst_atp_sync_media_to_training_schedule():
    """Sync media to training table according to environment-specific schedule."""
    return {}