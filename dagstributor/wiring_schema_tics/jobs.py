from dagster import job
from .ops import (
    test_db_connection_op,
    wst_atp_drop_op,
    # Backup ops
    wst_atp_bak_media_op,
    wst_atp_bak_prediction_op,
    wst_atp_bak_training_op,
    # Instantiate ops
    wst_atp_instantiate_media_op,
    wst_atp_instantiate_training_op,
    wst_atp_instantiate_prediction_op,
    wst_atp_set_perms_op,
    # Reload ops
    wst_atp_reload_media_op,
    wst_atp_reload_training_op,
    wst_atp_reload_prediction_op
)


@job(description="Test database connection with simple query")
def test_db_connection_job():
    """Job to test database connectivity."""
    test_db_connection_op()


@job(description="Execute all backup ATP scripts from sql/bak directory")
def wst_atp_bak_job():
    """Job to execute all backup ATP scripts simultaneously."""
    wst_atp_bak_media_op()
    wst_atp_bak_prediction_op()
    wst_atp_bak_training_op()


@job(description="Drop the atp schema - WARNING: This will delete all data!")
def wst_atp_drop_job():
    """Job to drop the atp schema and all its objects. Use with caution!"""
    wst_atp_drop_op()


@job(description="Instantiate the atp schema with all tables and permissions")
def wst_atp_instantiate_job():
    """Job to create the atp schema with media, training, prediction tables and set permissions."""
    wst_atp_instantiate_media_op()
    wst_atp_instantiate_training_op()
    wst_atp_instantiate_prediction_op()
    wst_atp_set_perms_op()


@job(description="Reload data from backup into atp schema tables")
def wst_atp_reload_job():
    """Job to restore data from backup tables into media, training, and prediction tables."""
    wst_atp_reload_media_op()
    wst_atp_reload_training_op()
    wst_atp_reload_prediction_op()


@job(description="Complete backup, drop, instantiate, and reload sequence")
def wst_atp_bak_drop_reload_job():
    """Job to execute complete backup, drop schema, recreate schema, and reload data sequence."""
    # Backup - these can run in parallel since they're independent
    media_bak = wst_atp_bak_media_op()
    prediction_bak = wst_atp_bak_prediction_op()
    training_bak = wst_atp_bak_training_op()
    
    # Drop schema - wait for all backups to complete
    drop_result = wst_atp_drop_op.after(media_bak, prediction_bak, training_bak)()
    
    # Instantiate schema and tables - run sequentially after drop
    media_instantiate = wst_atp_instantiate_media_op.after(drop_result)()
    training_instantiate = wst_atp_instantiate_training_op.after(media_instantiate)()
    prediction_instantiate = wst_atp_instantiate_prediction_op.after(training_instantiate)()
    perms_result = wst_atp_set_perms_op.after(prediction_instantiate)()
    
    # Reload data from backups - run after schema is fully instantiated
    wst_atp_reload_media_op.after(perms_result)()
    wst_atp_reload_training_op.after(perms_result)()
    wst_atp_reload_prediction_op.after(perms_result)()