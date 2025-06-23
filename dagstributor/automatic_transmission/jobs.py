"""Job definitions for automatic transmission pipeline."""

from dagster import job

# Global job configuration  
JOB_CONFIG = {
    "tags": {
        "dagster/max_runtime": "60",  # 60 seconds - informational only, not enforced
    }
}

from .ops import (
    at_01_rss_ingest_op,
    at_02_collect_op,
    at_03_parse_op,
    at_04_file_filtration_op,
    at_05_metadata_collection_op,
    at_06_media_filtration_op,
    at_07_initiation_op,
    at_08_download_check_op,
    at_09_transfer_op,
    at_10_cleanup_op,
)


@job(**JOB_CONFIG)
def at_01_rss_ingest_job():
    """RSS Ingest job - runs at :00 minutes."""
    at_01_rss_ingest_op()


@job(**JOB_CONFIG)
def at_02_collect_job():
    """Collect job - runs at :06 minutes."""
    at_02_collect_op()


@job(**JOB_CONFIG)
def at_03_parse_job():
    """Parse job - runs at :12 minutes."""
    at_03_parse_op()


@job(**JOB_CONFIG)
def at_04_file_filtration_job():
    """File Filtration job - runs at :18 minutes."""
    at_04_file_filtration_op()


@job(**JOB_CONFIG)
def at_05_metadata_collection_job():
    """Metadata Collection job - runs at :24 minutes."""
    at_05_metadata_collection_op()


@job(**JOB_CONFIG)
def at_06_media_filtration_job():
    """Media Filtration job - runs at :30 minutes."""
    at_06_media_filtration_op()


@job(**JOB_CONFIG)
def at_07_initiation_job():
    """Initiation job - runs at :36 minutes."""
    at_07_initiation_op()


@job(**JOB_CONFIG)
def at_08_download_check_job():
    """Download Check job - runs at :42 minutes."""
    at_08_download_check_op()


@job(**JOB_CONFIG)
def at_09_transfer_job():
    """Transfer job - runs at :48 minutes."""
    at_09_transfer_op()


@job(**JOB_CONFIG)
def at_10_cleanup_job():
    """Cleanup job - runs at :54 minutes."""
    at_10_cleanup_op()


# Full pipeline job configuration with extended timeout
FULL_PIPELINE_JOB_CONFIG = {
    "tags": {
        "dagster/max_runtime": "600",  # 10 minutes for all ops
    }
}


@job(**FULL_PIPELINE_JOB_CONFIG)
def at_full_pipeline_job():
    """Complete Automatic Transmission pipeline - runs all 10 ops sequentially.
    
    This job runs the entire AT pipeline from start to finish:
    1. RSS Ingest
    2. Collect
    3. Parse
    4. File Filtration
    5. Metadata Collection
    6. Media Filtration
    7. Initiation
    8. Download Check
    9. Transfer
    10. Cleanup
    
    Note: This job is designed for manual/on-demand execution only.
    """
    at_01_rss_ingest_op()
    at_02_collect_op()
    at_03_parse_op()
    at_04_file_filtration_op()
    at_05_metadata_collection_op()
    at_06_media_filtration_op()
    at_07_initiation_op()
    at_08_download_check_op()
    at_09_transfer_op()
    at_10_cleanup_op()