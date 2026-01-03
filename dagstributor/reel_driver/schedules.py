"""Schedule definitions for reel-driver pipeline."""

import os
import yaml
from pathlib import Path
from dagster import schedule, DefaultScheduleStatus

from .jobs import reel_driver_training_gpu_job, reel_driver_training_cpu_job, reel_driver_review_all_job


def _get_environment():
    """Get and validate ENVIRONMENT variable."""
    env = os.environ.get('ENVIRONMENT')
    if not env:
        raise ValueError(
            "ENVIRONMENT variable is not set. "
            "This must be set to 'dev', 'stg', or 'prod'."
        )
    return env


# Load configuration at module level for use in decorators
def _load_config():
    environment = _get_environment()
    config_dir = Path(__file__).parent.parent.parent / 'config' / 'schedules'
    
    # Load environment-specific configuration
    env_config_path = config_dir / f'{environment}.yaml'
    if not env_config_path.exists():
        raise FileNotFoundError(
            f"Schedule configuration not found for environment '{environment}' at {env_config_path}. "
            f"Please create a configuration file for this environment."
        )
    
    with open(env_config_path, 'r') as f:
        config = yaml.safe_load(f)
        if not config or 'schedules' not in config:
            raise ValueError(
                f"Invalid configuration in {env_config_path}. "
                f"Configuration must contain a 'schedules' section."
            )
        return config


# Global config loaded at import time
CONFIG = _load_config()

# List to collect schedules that should be exposed
schedules = []

# GPU training schedule
@schedule(
    job=reel_driver_training_gpu_job,
    cron_schedule=CONFIG["schedules"]["reel_driver_training_gpu"]["cron_schedule"],
    name="reel_driver_training_gpu_schedule",
    default_status=getattr(DefaultScheduleStatus, CONFIG["schedules"]["reel_driver_training_gpu"]["default_status"])
)
def reel_driver_training_gpu_schedule():
    """Reel Driver GPU training pipeline - scheduled on Fridays (dev), Saturdays (stg), Sundays (prod)."""
    return {}

schedules.append(reel_driver_training_gpu_schedule)

# CPU training schedule - only in dev and stg
if "reel_driver_training_cpu" in CONFIG["schedules"]:
    @schedule(
        job=reel_driver_training_cpu_job,
        cron_schedule=CONFIG["schedules"]["reel_driver_training_cpu"]["cron_schedule"],
        name="reel_driver_training_cpu_schedule",
        default_status=getattr(DefaultScheduleStatus, CONFIG["schedules"]["reel_driver_training_cpu"]["default_status"])
    )
    def reel_driver_training_cpu_schedule():
        """Reel Driver CPU training pipeline - scheduled on Tuesdays (dev), Wednesdays (stg)."""
        return {}

    schedules.append(reel_driver_training_cpu_schedule)

# Conditionally create review_all schedule only if it exists in config
if "reel_driver_review_all" in CONFIG["schedules"]:
    @schedule(
        job=reel_driver_review_all_job,
        cron_schedule=CONFIG["schedules"]["reel_driver_review_all"]["cron_schedule"],
        name="reel_driver_review_all_schedule",
        default_status=getattr(DefaultScheduleStatus, CONFIG["schedules"]["reel_driver_review_all"]["default_status"])
    )
    def reel_driver_review_all_schedule():
        """Reel Driver review all runs daily at 05:00 CT."""
        return {}
    
    schedules.append(reel_driver_review_all_schedule)
