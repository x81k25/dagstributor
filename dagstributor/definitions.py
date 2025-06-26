"""Central Dagster definitions for the dagstributor project."""

from dagster import Definitions
from .resources import postgres_resource

# All assets have been removed
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
    at_full_pipeline_job,
)
from .wiring_schema_tics.jobs import test_db_connection_job
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

# All assets have been removed
all_assets = []

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
    at_full_pipeline_job,
]

# Define wiring schema-tics jobs
ws_jobs = [
    test_db_connection_job,
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

# Configure resources
resources = {
    "postgres": postgres_resource.configured({
        "host": {"env": "WST_PGSQL_HOST"},
        "port": {"env": "WST_PGSQL_PORT"},
        "database": {"env": "WST_PGSQL_DATABASE"},
        "user": {"env": "WST_PGSQL_USERNAME"},
        "password": {"env": "WST_PGSQL_PASSWORD"},
    })
}

# Create the Definitions object
defs = Definitions(
    assets=all_assets,
    jobs=at_jobs + ws_jobs,
    schedules=at_schedules,
    resources=resources,
)