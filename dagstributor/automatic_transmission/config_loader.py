import os
import yaml
from pathlib import Path


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