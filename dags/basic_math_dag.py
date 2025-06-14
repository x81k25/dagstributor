"""Basic math operations DAG for testing repository connection."""

import random
from dagster import asset, job, op, Definitions


@op
def generate_numbers():
    """Generate random numbers for calculation."""
    numbers = [random.randint(1, 100) for _ in range(5)]
    print(f"Generated numbers: {numbers}")
    return numbers


@op
def calculate_sum(numbers):
    """Calculate sum of numbers."""
    total = sum(numbers)
    print(f"Sum: {total}")
    return total


@op
def calculate_average(numbers):
    """Calculate average of numbers."""
    avg = sum(numbers) / len(numbers)
    print(f"Average: {avg}")
    return avg


@op
def compare_values(total, average):
    """Compare total and average values."""
    comparison = "greater" if total > average else "less or equal"
    result = f"Total ({total}) is {comparison} than average ({average})"
    print(result)
    return result


@job
def basic_math_job():
    """Basic math operations job."""
    numbers = generate_numbers()
    total = calculate_sum(numbers)
    avg = calculate_average(numbers)
    compare_values(total, avg)


# Asset-based approach
@asset
def random_dataset():
    """Generate a random dataset."""
    return [random.randint(1, 1000) for _ in range(10)]


@asset
def dataset_stats(random_dataset):
    """Calculate statistics for the dataset."""
    stats = {
        "count": len(random_dataset),
        "sum": sum(random_dataset),
        "min": min(random_dataset),
        "max": max(random_dataset),
        "average": sum(random_dataset) / len(random_dataset)
    }
    print(f"Dataset statistics: {stats}")
    return stats


@asset
def dataset_analysis(dataset_stats):
    """Analyze the dataset statistics."""
    analysis = {
        "range": dataset_stats["max"] - dataset_stats["min"],
        "above_average": dataset_stats["sum"] > dataset_stats["average"] * dataset_stats["count"],
        "variance_indicator": "high" if dataset_stats["max"] - dataset_stats["min"] > 500 else "low"
    }
    print(f"Analysis results: {analysis}")
    return analysis


# Definitions for this module
defs = Definitions(
    assets=[random_dataset, dataset_stats, dataset_analysis],
    jobs=[basic_math_job],
)