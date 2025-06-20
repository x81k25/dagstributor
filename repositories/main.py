"""Main repository definition for dagstributor project."""

from dagster import repository

from dagstributor.definitions import defs
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
from dagstributor.test_k8s_manifest.ops import test_k8s_manifest_job
from dagstributor.test_k8s_manifest.simple_k8s_test import test_k8s_simple_job
from dagstributor.test_k8s_manifest.test_with_existing import test_existing_k8s_job


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
        # Test k8s manifest job
        test_k8s_manifest_job,
        test_k8s_simple_job,
        test_existing_k8s_job,
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
    ]