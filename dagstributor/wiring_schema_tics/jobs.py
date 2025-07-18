from dagster import job
from .ops import (
    # Backup ops
    wst_atp_bak_media_op,
    wst_atp_bak_prediction_op,
    wst_atp_bak_training_op,
    # Reload ops
    wst_atp_reload_media_op,
    wst_atp_reload_training_op,
    wst_atp_reload_prediction_op,
    # Sync ops
    wst_atp_sync_media_to_training_op,
)


@job(description="Execute all backup ATP scripts from sql/bak directory", tags={"service": "wst", "schema": "atp", "function": "bak"})
def wst_atp_bak_job():
    """Job to execute all backup ATP scripts simultaneously."""
    wst_atp_bak_media_op()
    wst_atp_bak_prediction_op()
    wst_atp_bak_training_op()


@job(description="Reload data from backup into atp schema tables", tags={"service": "wst", "schema": "atp", "function": "bak"})
def wst_atp_reload_job():
    """Job to restore data from backup tables into media, training, and prediction tables."""
    wst_atp_reload_media_op()
    wst_atp_reload_training_op()
    wst_atp_reload_prediction_op()


@job(description="Sync media data to training table based on rejection status", tags={"service": "wst", "function": "sync"})
def wst_atp_sync_media_to_training_job():
    """Job to sync media records to training table, setting labels based on rejection status."""
    wst_atp_sync_media_to_training_op()

