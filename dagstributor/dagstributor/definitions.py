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
    at_01_rss_ingest_job,
    at_02_collect_job,
    at_03_parse_job,
    at_04_file_filtration_job,
    at_05_metadata_collection_job,
    at_06_media_filtration_job,
    at_07_initiation_job,
    at_08_download_check_job,
    at_09_transfer_job,
    at_10_cleanup_job,
)
from .automatic_transmission.schedules import (
    at_01_rss_ingest_schedule,
    at_02_collect_schedule,
    at_03_parse_schedule,
    at_04_file_filtration_schedule,
    at_05_metadata_collection_schedule,
    at_06_media_filtration_schedule,
    at_07_initiation_schedule,
    at_08_download_check_schedule,
    at_09_transfer_schedule,
    at_10_cleanup_schedule,
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
    at_01_rss_ingest_job,
    at_02_collect_job,
    at_03_parse_job,
    at_04_file_filtration_job,
    at_05_metadata_collection_job,
    at_06_media_filtration_job,
    at_07_initiation_job,
    at_08_download_check_job,
    at_09_transfer_job,
    at_10_cleanup_job,
]

# Define all automatic transmission schedules
at_schedules = [
    at_01_rss_ingest_schedule,
    at_02_collect_schedule,
    at_03_parse_schedule,
    at_04_file_filtration_schedule,
    at_05_metadata_collection_schedule,
    at_06_media_filtration_schedule,
    at_07_initiation_schedule,
    at_08_download_check_schedule,
    at_09_transfer_schedule,
    at_10_cleanup_schedule,
]

# Create the Definitions object
defs = Definitions(
    assets=all_assets,
    jobs=at_jobs,
    schedules=at_schedules,
)