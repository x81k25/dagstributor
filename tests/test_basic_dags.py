"""Tests for basic DAGs to verify repository functionality."""

import pytest
from dagster import materialize, materialize_to_memory

from dags.basic_math_dag import (
    basic_math_job,
    random_dataset,
    dataset_stats,
    dataset_analysis
)
from dags.string_processing_dag import (
    string_processing_job,
    raw_messages,
    processed_messages,
    message_summary
)
from dags.simple_workflow_dag import (
    simple_workflow_job,
    workflow_metadata,
    execution_context,
    execution_report
)


class TestBasicMathDAG:
    """Test basic math DAG functionality."""
    
    def test_basic_math_job_execution(self):
        """Test that basic math job executes successfully."""
        result = basic_math_job.execute_in_process()
        assert result.success
    
    def test_random_dataset_asset(self):
        """Test random dataset asset creation."""
        result = materialize_to_memory([random_dataset])
        assert result.success
        dataset = result.output_for_node("random_dataset")
        assert isinstance(dataset, list)
        assert len(dataset) == 10
        assert all(isinstance(x, int) for x in dataset)
    
    def test_dataset_stats_asset(self):
        """Test dataset statistics calculation."""
        result = materialize_to_memory([random_dataset, dataset_stats])
        assert result.success
        stats = result.output_for_node("dataset_stats")
        assert isinstance(stats, dict)
        assert "count" in stats
        assert "sum" in stats
        assert "average" in stats
        assert stats["count"] == 10


class TestStringProcessingDAG:
    """Test string processing DAG functionality."""
    
    def test_string_processing_job_execution(self):
        """Test that string processing job executes successfully."""
        result = string_processing_job.execute_in_process()
        assert result.success
    
    def test_raw_messages_asset(self):
        """Test raw messages asset creation."""
        result = materialize_to_memory([raw_messages])
        assert result.success
        messages = result.output_for_node("raw_messages")
        assert isinstance(messages, list)
        assert len(messages) == 7
        assert all(isinstance(msg, str) for msg in messages)
    
    def test_processed_messages_asset(self):
        """Test message processing functionality."""
        result = materialize_to_memory([raw_messages, processed_messages])
        assert result.success
        processed = result.output_for_node("processed_messages")
        assert isinstance(processed, list)
        assert len(processed) == 7
        assert all("message" in item for item in processed)
        assert all("word_count" in item for item in processed)


class TestSimpleWorkflowDAG:
    """Test simple workflow DAG functionality."""
    
    def test_simple_workflow_job_execution(self):
        """Test that simple workflow job executes successfully."""
        result = simple_workflow_job.execute_in_process()
        assert result.success
    
    def test_workflow_metadata_asset(self):
        """Test workflow metadata asset creation."""
        result = materialize_to_memory([workflow_metadata])
        assert result.success
        metadata = result.output_for_node("workflow_metadata")
        assert isinstance(metadata, dict)
        assert "created_at" in metadata
        assert "version" in metadata
        assert "features" in metadata
    
    def test_execution_context_asset(self):
        """Test execution context asset creation."""
        result = materialize_to_memory([workflow_metadata, execution_context])
        assert result.success
        context = result.output_for_node("execution_context")
        assert isinstance(context, dict)
        assert "execution_id" in context
        assert "metadata" in context
        assert "runtime_config" in context
    
    def test_execution_report_asset(self):
        """Test execution report asset creation."""
        result = materialize_to_memory([
            workflow_metadata, 
            execution_context, 
            execution_report
        ])
        assert result.success
        report = result.output_for_node("execution_report")
        assert isinstance(report, dict)
        assert "execution_id" in report
        assert "status" in report
        assert "metrics" in report
        assert report["status"] == "success"


class TestRepositoryIntegration:
    """Test repository-level integration."""
    
    def test_all_assets_can_materialize(self):
        """Test that all assets can be materialized together."""
        all_assets = [
            # Basic math assets
            random_dataset, dataset_stats, dataset_analysis,
            # String processing assets
            raw_messages, processed_messages, message_summary,
            # Workflow assets
            workflow_metadata, execution_context, execution_report
        ]
        
        result = materialize_to_memory(all_assets)
        assert result.success
    
    def test_repository_contains_all_jobs(self):
        """Test that repository contains all expected jobs."""
        from repositories.main import dagstributor_repository
        
        repo = dagstributor_repository
        job_names = [job.name for job in repo.get_all_jobs()]
        
        expected_jobs = [
            "example_dag",
            "basic_math_job", 
            "string_processing_job",
            "simple_workflow_job"
        ]
        
        for expected_job in expected_jobs:
            assert expected_job in job_names