"""Main repository definition for Dagstributor DAGs."""

from dagster import repository

from dags.example_dag import example_dag
from dags.basic_math_dag import basic_math_job
from dags.string_processing_dag import string_processing_job
from dags.simple_workflow_dag import simple_workflow_job


@repository
def dagstributor_repository():
    """Repository containing all Dagstributor DAGs, schedules, and sensors."""
    return [
        example_dag,
        basic_math_job,
        string_processing_job,
        simple_workflow_job,
    ]