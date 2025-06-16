"""Central Dagster definitions for the dagstributor project."""

from dagster import Definitions

from .assets import (
    raw_data,
    processed_data,
    random_dataset,
    dataset_stats,
    dataset_analysis,
    raw_messages,
    processed_messages,
    message_summary,
    workflow_metadata,
    execution_context,
    execution_report,
)

# Define all assets in the project
all_assets = [
    # Example DAG assets
    raw_data,
    processed_data,
    # Basic math DAG assets
    random_dataset,
    dataset_stats,
    dataset_analysis,
    # String processing DAG assets
    raw_messages,
    processed_messages,
    message_summary,
    # Simple workflow DAG assets
    workflow_metadata,
    execution_context,
    execution_report,
]

# Create the Definitions object
defs = Definitions(
    assets=all_assets,
)