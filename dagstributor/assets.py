"""Consolidated assets from all DAG files."""

import random
import time
import json
from datetime import datetime
from dagster import asset

# Assets from example_dag.py

@asset
def raw_data():
    """Raw data asset."""
    return [1, 2, 3, 4, 5]


@asset
def processed_data(raw_data):
    """Processed data asset."""
    return [x * 2 for x in raw_data]


# Assets from basic_math_dag.py

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


# Assets from string_processing_dag.py

@asset
def raw_messages():
    """Generate raw message data."""
    messages = [
        f"Message {i}: Current timestamp {int(time.time()) + i}"
        for i in range(1, 8)
    ]
    return messages


@asset
def processed_messages(raw_messages):
    """Process raw messages."""
    processed = []
    for msg in raw_messages:
        words = msg.split()
        processed.append({
            "message": msg,
            "word_count": len(words),
            "first_word": words[0] if words else "",
            "last_word": words[-1] if words else "",
            "contains_timestamp": "timestamp" in msg.lower()
        })
    
    print(f"Processed {len(processed)} messages")
    return processed


@asset
def message_summary(processed_messages):
    """Create summary of processed messages."""
    summary = {
        "total_messages": len(processed_messages),
        "messages_with_timestamp": sum(1 for msg in processed_messages if msg["contains_timestamp"]),
        "total_words": sum(msg["word_count"] for msg in processed_messages),
        "unique_first_words": len(set(msg["first_word"] for msg in processed_messages))
    }
    
    print(f"Message summary: {summary}")
    return summary


# Assets from simple_workflow_dag.py

@asset
def workflow_metadata():
    """Generate workflow metadata."""
    metadata = {
        "created_at": datetime.now().isoformat(),
        "version": "1.0.0",
        "environment": "development",
        "features": ["basic_ops", "asset_management", "workflow_orchestration"]
    }
    return metadata


@asset
def execution_context(workflow_metadata):
    """Create execution context based on metadata."""
    context = {
        "execution_id": f"exec_{int(datetime.now().timestamp())}",
        "metadata": workflow_metadata,
        "runtime_config": {
            "parallel_execution": False,
            "resource_allocation": "standard",
            "monitoring_enabled": True
        }
    }
    print(f"Created execution context: {context['execution_id']}")
    return context


@asset
def execution_report(execution_context):
    """Generate execution report."""
    report = {
        "execution_id": execution_context["execution_id"],
        "report_generated_at": datetime.now().isoformat(),
        "environment": execution_context["metadata"]["environment"],
        "features_used": execution_context["metadata"]["features"],
        "status": "success",
        "metrics": {
            "assets_processed": 3,
            "execution_time_ms": 250,
            "memory_usage_mb": 45
        }
    }
    
    print(f"Generated execution report for: {report['execution_id']}")
    return report