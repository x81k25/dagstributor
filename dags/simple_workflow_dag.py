"""Simple workflow DAG for testing basic operations."""

import json
from datetime import datetime
from dagster import asset, job, op, Definitions


@op
def create_workflow_config():
    """Create a simple workflow configuration."""
    config = {
        "workflow_id": f"test_workflow_{int(datetime.now().timestamp())}",
        "created_at": datetime.now().isoformat(),
        "steps": ["initialize", "process", "validate", "complete"],
        "settings": {
            "max_retries": 3,
            "timeout_seconds": 300,
            "log_level": "INFO"
        }
    }
    print(f"Created workflow config: {config['workflow_id']}")
    return config


@op
def initialize_workflow(config):
    """Initialize the workflow."""
    print(f"Initializing workflow: {config['workflow_id']}")
    initialization_data = {
        "status": "initialized",
        "start_time": datetime.now().isoformat(),
        "workflow_id": config["workflow_id"],
        "step_count": len(config["steps"])
    }
    print(f"Workflow initialized with {initialization_data['step_count']} steps")
    return initialization_data


@op
def process_workflow_steps(init_data):
    """Process workflow steps."""
    print(f"Processing steps for workflow: {init_data['workflow_id']}")
    
    step_results = []
    for i, step in enumerate(["initialize", "process", "validate", "complete"]):
        step_result = {
            "step_number": i + 1,
            "step_name": step,
            "executed_at": datetime.now().isoformat(),
            "duration_ms": (i + 1) * 100,  # Simulate processing time
            "status": "completed"
        }
        step_results.append(step_result)
        print(f"Completed step {i + 1}: {step}")
    
    return {
        "workflow_id": init_data["workflow_id"],
        "steps_executed": len(step_results),
        "step_results": step_results,
        "total_duration_ms": sum(step["duration_ms"] for step in step_results)
    }


@op
def finalize_workflow(processed_steps):
    """Finalize the workflow."""
    workflow_summary = {
        "workflow_id": processed_steps["workflow_id"],
        "status": "completed",
        "completed_at": datetime.now().isoformat(),
        "steps_executed": processed_steps["steps_executed"],
        "total_duration_ms": processed_steps["total_duration_ms"],
        "success": True
    }
    
    print(f"Workflow finalized: {workflow_summary['workflow_id']}")
    print(f"Total execution time: {workflow_summary['total_duration_ms']}ms")
    return workflow_summary


@job
def simple_workflow_job():
    """Simple workflow orchestration job."""
    config = create_workflow_config()
    init_data = initialize_workflow(config)
    processed = process_workflow_steps(init_data)
    finalize_workflow(processed)


# Asset-based approach
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


# Definitions for this module
defs = Definitions(
    assets=[workflow_metadata, execution_context, execution_report],
    jobs=[simple_workflow_job],
)