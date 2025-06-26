from dagster import job
from .ops import test_db_connection_op


@job(description="Test database connection with simple query")
def test_db_connection_job():
    """Job to test database connectivity."""
    test_db_connection_op()