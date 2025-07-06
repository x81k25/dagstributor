"""Central Dagster definitions for the dagstributor project."""

from dagster import Definitions, EnvVar
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
from .wiring_schema_tics.jobs import (
    test_db_connection_job, 
    wst_atp_bak_job, 
    wst_atp_drop_job,
    wst_atp_instantiate_job,
    wst_atp_reload_job,
    wst_atp_bak_drop_reload_job,
    wst_atp_sync_media_to_training_job,
    test_schmest_job,
    testy_mctestface_job,
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
from .wiring_schema_tics.schedules import (
    wst_atp_bak_schedule,
    wst_atp_sync_media_to_training_schedule,
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
wst_jobs = [
    test_db_connection_job,
    wst_atp_bak_job,
    wst_atp_drop_job,
    wst_atp_instantiate_job,
    wst_atp_reload_job,
    wst_atp_bak_drop_reload_job,
    wst_atp_sync_media_to_training_job,
    test_schmest_job,
    testy_mctestface_job,
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

# Define wiring schema-tics schedules
wst_schedules = [
    wst_atp_bak_schedule,
    wst_atp_sync_media_to_training_schedule,
]

# Configure resources
resources = {
    "postgres": postgres_resource
}

# Create the Definitions object
defs = Definitions(
    assets=all_assets,
    jobs=at_jobs + wst_jobs,
    schedules=at_schedules + wst_schedules,
    resources=resources,
)