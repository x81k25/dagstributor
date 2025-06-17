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
from .automatic_transmission.jobs import (
    rss_ingest_job,
    collect_job,
    parse_job,
    file_filtration_job,
    metadata_collection_job,
    media_filtration_job,
    initiation_job,
    download_check_job,
    transfer_job,
    cleanup_job,
)
from .automatic_transmission.schedules import (
    rss_ingest_schedule,
    collect_schedule,
    parse_schedule,
    file_filtration_schedule,
    metadata_collection_schedule,
    media_filtration_schedule,
    initiation_schedule,
    download_check_schedule,
    transfer_schedule,
    cleanup_schedule,
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

# Define all automatic transmission jobs
at_jobs = [
    rss_ingest_job,
    collect_job,
    parse_job,
    file_filtration_job,
    metadata_collection_job,
    media_filtration_job,
    initiation_job,
    download_check_job,
    transfer_job,
    cleanup_job,
]

# Define all automatic transmission schedules
at_schedules = [
    rss_ingest_schedule,
    collect_schedule,
    parse_schedule,
    file_filtration_schedule,
    metadata_collection_schedule,
    media_filtration_schedule,
    initiation_schedule,
    download_check_schedule,
    transfer_schedule,
    cleanup_schedule,
]

# Create the Definitions object
defs = Definitions(
    assets=all_assets,
    jobs=at_jobs,
    schedules=at_schedules,
)