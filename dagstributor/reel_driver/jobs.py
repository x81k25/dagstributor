"""Job definitions for reel-driver pipeline."""

from dagster import job

# Global job configuration  
JOB_CONFIG = {
    "tags": {
        "dagster/max_runtime": "43200",  # 12 hours for ML training pipeline
        "service": "reel-driver",
    }
}

from .ops import (
    reel_driver_feature_engineering_cpu_op,
    reel_driver_model_training_cpu_op,
    reel_driver_feature_engineering_gpu_op,
    reel_driver_model_training_gpu_op,
    reel_driver_review_all_op,
)


@job(**JOB_CONFIG)
def reel_driver_training_cpu_job():
    """Reel Driver CPU training pipeline - runs feature engineering then model training sequentially.

    This job runs the reel-driver training pipeline on CPU:
    1. Feature Engineering (CPU)
    2. Model Training (CPU)

    The pipeline will fail fast - if feature engineering fails, model training will not run.
    This job is available on-demand only (no schedule).
    """
    feature_engineering = reel_driver_feature_engineering_cpu_op()
    reel_driver_model_training_cpu_op(feature_engineering)


@job(**JOB_CONFIG)
def reel_driver_training_gpu_job():
    """Reel Driver GPU training pipeline - runs feature engineering then model training sequentially.

    This job runs the reel-driver training pipeline on GPU (RTX 3060):
    1. Feature Engineering (GPU)
    2. Model Training (GPU)

    The pipeline will fail fast - if feature engineering fails, model training will not run.
    This job runs on the scheduled training timeslot.
    """
    feature_engineering = reel_driver_feature_engineering_gpu_op()
    reel_driver_model_training_gpu_op(feature_engineering)


@job(**JOB_CONFIG)
def reel_driver_review_all_job():
    """Reset the reviewed flag for all training records.
    
    This job executes a simple SQL statement to update the atp.training table,
    setting reviewed = False for all records.
    """
    reel_driver_review_all_op()