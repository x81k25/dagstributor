from dagster import job
from .ops import test_db_connection_op, wst_atp_bak_op, wst_atp_drop_op, wst_atp_instantiate_op, wst_atp_reload_op


@job(description="Test database connection with simple query")
def test_db_connection_job():
    """Job to test database connectivity."""
    test_db_connection_op()


@job(description="Execute all backup ATP scripts from sql/bak directory")
def wst_atp_bak_job():
    """Job to execute all backup ATP scripts."""
    wst_atp_bak_op()


@job(description="Drop the atp schema - WARNING: This will delete all data!")
def wst_atp_drop_job():
    """Job to drop the atp schema and all its objects. Use with caution!"""
    wst_atp_drop_op()


@job(description="Instantiate the atp schema with all tables and permissions")
def wst_atp_instantiate_job():
    """Job to create the atp schema with media, training, prediction tables and set permissions."""
    wst_atp_instantiate_op()


@job(description="Reload data from backup into atp schema tables")
def wst_atp_reload_job():
    """Job to restore data from backup tables into media, training, and prediction tables."""
    wst_atp_reload_op()