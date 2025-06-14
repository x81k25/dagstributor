"""Example DAG demonstrating basic Dagster patterns."""

from dagster import Config, asset, job, op, Definitions


@op
def extract_data():
    """Extract data from source."""
    return [1, 2, 3, 4, 5]


@op
def transform_data(data):
    """Transform the extracted data."""
    return [x * 2 for x in data]


@op
def load_data(transformed_data):
    """Load the transformed data."""
    print(f"Loading data: {transformed_data}")
    return len(transformed_data)


@job
def example_dag():
    """Example DAG job."""
    raw_data = extract_data()
    transformed = transform_data(raw_data)
    load_data(transformed)


# Asset-based approach (recommended for modern Dagster)
@asset
def raw_data():
    """Raw data asset."""
    return [1, 2, 3, 4, 5]


@asset
def processed_data(raw_data):
    """Processed data asset."""
    return [x * 2 for x in raw_data]


# Definitions for this module
defs = Definitions(
    assets=[raw_data, processed_data],
    jobs=[example_dag],
)