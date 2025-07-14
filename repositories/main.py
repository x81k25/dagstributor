"""Main repository definition for dagstributor project."""

from dagster import repository
from dagstributor.resources import postgres_resource

from dagstributor.automatic_transmission.jobs import (
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
from dagstributor.wiring_schema_tics.jobs import (
    wst_atp_bak_job,
    wst_atp_reload_job,
    wst_atp_sync_media_to_training_job,
)
from dagstributor.test_jobs.jobs import (
    test_db_connection_job,
    test_timeout_conditions_job,
)
from dagstributor.reel_driver.jobs import (
    reel_driver_training_job,
    reel_driver_review_all_job,
)
from dagstributor.automatic_transmission.schedules import (
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
from dagstributor.wiring_schema_tics.schedules import (
    wst_atp_bak_schedule,
    wst_atp_sync_media_to_training_schedule,
)
from dagstributor.reel_driver.schedules import (
    reel_driver_training_schedule,
    reel_driver_review_all_schedule,
)


@repository
def dagstributor_repo():
    """Main repository containing all jobs and schedules."""
    return [
        # Automatic transmission jobs
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
        # Wiring schema-tics (wst) jobs
        wst_atp_bak_job,
        wst_atp_reload_job,
        wst_atp_sync_media_to_training_job,
        # Test jobs
        test_db_connection_job,
        test_timeout_conditions_job,
        # Reel driver jobs
        reel_driver_training_job,
        reel_driver_review_all_job,
        # Automatic transmission schedules
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
        # Wiring schema-tics schedules
        wst_atp_bak_schedule,
        wst_atp_sync_media_to_training_schedule,
        # Reel driver schedules
        reel_driver_training_schedule,
        reel_driver_review_all_schedule,
    ]