"""Test job definitions."""

from dagster import job
from .ops import test_db_connection_op, test_timeout_conditions_op


@job(
    description="Test database connection with simple query",
    tags={"service": "test", "function": "test"}
)
def test_db_connection_job():
    """Job to test database connectivity."""
    test_db_connection_op()


@job(
    description="Test job that sleeps for 60 minutes to test timeout configurations",
    tags={"service": "test", "function": "test"}
)
def test_timeout_conditions_job():
    """Job that sleeps for 60 minutes to test timeout configurations."""
    test_timeout_conditions_op()