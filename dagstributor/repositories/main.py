"""Main repository definition for dagstributor project."""

from dagster import repository

from ..dagstributor.definitions import defs
from ..dagstributor.automatic_transmission.jobs import (
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
from ..dagstributor.automatic_transmission.schedules import (
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


@repository
def dagstributor_repo():
    """Main repository containing all jobs and schedules."""
    return [
        # Automatic transmission jobs
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
        # Automatic transmission schedules
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
    ] + defs.get_all_asset_checks() + defs.get_all_job_definitions() + list(defs.get_all_asset_specs())