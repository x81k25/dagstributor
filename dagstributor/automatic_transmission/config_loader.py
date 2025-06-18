import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path


# Load configuration at module level for use in decorators
def _load_config():
    environment = os.getenv('ENVIRONMENT', 'dev')
    config_dir = Path(__file__).parent.parent.parent / 'config' / 'schedules'
    
    # Load environment-specific configuration
    env_config_path = config_dir / f'{environment}.yaml'
    if env_config_path.exists():
        with open(env_config_path, 'r') as f:
            return yaml.safe_load(f) or {}
    else:
        # Fall back to base config if environment config doesn't exist
        base_config_path = config_dir / 'base.yaml'
        if base_config_path.exists():
            with open(base_config_path, 'r') as f:
                return yaml.safe_load(f) or {}
    return {}


# Global config loaded at import time
CONFIG = _load_config()


def load_schedule_config(environment: Optional[str] = None) -> Dict[str, Any]:
    """
    Load schedule configuration for the specified environment.
    
    Args:
        environment: The environment to load config for (dev, stg, prod).
                    If None, uses ENVIRONMENT env var or defaults to 'dev'.
    
    Returns:
        Dict containing merged schedule configuration
    """
    if environment is None:
        environment = os.getenv('ENVIRONMENT', 'dev')
    
    # Get the config directory path
    config_dir = Path(__file__).parent.parent.parent / 'config' / 'schedules'
    
    # Load base configuration
    base_config_path = config_dir / 'base.yaml'
    if base_config_path.exists():
        with open(base_config_path, 'r') as f:
            base_config = yaml.safe_load(f) or {}
    else:
        base_config = {}
    
    # Load environment-specific configuration
    env_config_path = config_dir / f'{environment}.yaml'
    if env_config_path.exists():
        with open(env_config_path, 'r') as f:
            env_config = yaml.safe_load(f) or {}
    else:
        print(f"Warning: No configuration found for environment '{environment}', using base config only")
        env_config = {}
    
    # Merge configurations (environment config overrides base config)
    merged_config = deep_merge(base_config, env_config)
    
    return merged_config


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge two dictionaries, with override values taking precedence.
    
    Args:
        base: Base dictionary
        override: Dictionary with values to override
    
    Returns:
        Merged dictionary
    """
    result = base.copy()
    
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    
    return result


def get_schedule_config(schedule_name: str, environment: Optional[str] = None) -> Dict[str, Any]:
    """
    Get configuration for a specific schedule.
    
    Args:
        schedule_name: Name of the schedule (e.g., 'at_01_rss_ingest')
        environment: The environment to load config for
    
    Returns:
        Dict containing schedule configuration
    """
    config = load_schedule_config(environment)
    schedules = config.get('schedules', {})
    
    return schedules.get(schedule_name, {})