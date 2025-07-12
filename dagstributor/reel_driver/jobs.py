"""Job definitions for reel-driver pipeline."""

from dagster import job

# Global job configuration  
JOB_CONFIG = {
    "tags": {
        "dagster/max_runtime": "7200",  # 2 hours for ML training pipeline
        "service": "reel-driver",
    }
}

from .ops import (
    reel_driver_training_feature_engineering_op,
    reel_driver_model_training_op,
)


@job(**JOB_CONFIG)
def reel_driver_training_pipeline_job():
    """Complete Reel Driver training pipeline - runs feature engineering then model training sequentially.
    
    This job runs the reel-driver training pipeline:
    1. Training Feature Engineering
    2. Model Training
    
    The pipeline will fail fast - if feature engineering fails, model training will not run.
    """
    # Chain ops sequentially - model training waits for feature engineering to complete
    feature_engineering = reel_driver_training_feature_engineering_op()
    reel_driver_model_training_op(feature_engineering)