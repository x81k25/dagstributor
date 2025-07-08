"""Test operations for timeout condition testing."""

import time
from dagster import op, Out, Output, job, schedule, DefaultScheduleStatus
from .automatic_transmission.config_loader import CONFIG

@op(out=Out(dict))
def test_timeout_conditions_op(context):
    """Test op that sleeps for 60 minutes to test timeout configurations."""
    context.log.info("Starting test_timeout_conditions op - will sleep for 60 minutes (3600 seconds)")
    
    try:
        # Sleep for 60 minutes
        time.sleep(3600)
        
        context.log.info("Test timeout conditions op completed successfully after 60 minutes")
        
        return Output(
            value={
                "status": "success",
                "sleep_duration_seconds": 3600,
                "sleep_duration_minutes": 60
            },
            metadata={
                "sleep_duration_seconds": 3600,
                "sleep_duration_minutes": 60,
                "test_purpose": "timeout_testing"
            }
        )
        
    except Exception as e:
        context.log.error(f"Test timeout conditions op failed: {str(e)}")
        return Output(
            value={"status": "failed", "error": str(e)},
            metadata={"test_purpose": "timeout_testing"}
        )


@job(
    description="Test job that sleeps for 60 minutes to test timeout configurations",
    tags={"service": "test", "function": "test"}
)
def test_timeout_conditions_job():
    """Job that sleeps for 60 minutes to test timeout configurations."""
    test_timeout_conditions_op()


@schedule(
    job=test_timeout_conditions_job,
    cron_schedule=CONFIG["schedules"]["test_timeout_conditions"]["cron_schedule"],
    name="test_timeout_conditions_schedule",
    default_status=getattr(DefaultScheduleStatus, CONFIG["schedules"]["test_timeout_conditions"]["default_status"])
)
def test_timeout_conditions_schedule():
    """Test timeout conditions job runs according to environment-specific schedule."""
    return {}