"""Main repository definition for Dagstributor DAGs."""

from dagster import repository

from dags.example_dag import example_dag


@repository
def dagstributor_repository():
    """Repository containing all Dagstributor DAGs, schedules, and sensors."""
    return [
        example_dag,
    ]