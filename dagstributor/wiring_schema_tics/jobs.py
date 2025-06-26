from dagster import job
from .ops import test_db_connection_op, wst_bak_atp_op


@job(description="Test database connection with simple query")
def test_db_connection_job():
    """Job to test database connectivity."""
    test_db_connection_op()


@job(description="Execute all backup ATP scripts from sql/bak directory")
def wst_bak_atp_job():
    """Job to execute all backup ATP scripts."""
    wst_bak_atp_op()