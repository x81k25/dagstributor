"""Tests for modernized asset-based workflows."""

import pytest
from dagster import materialize_to_memory

from dagstributor.assets import (
    # Basic math assets
    random_dataset,
    dataset_stats,
    dataset_analysis,
    # String processing assets
    raw_messages,
    processed_messages,
    message_summary,
    # Workflow assets
    workflow_metadata,
    execution_context,
    execution_report,
    # Example assets
    raw_data,
    processed_data
)
from dagstributor.definitions import defs


class TestBasicMathAssets:
    """Test basic math assets functionality."""
    
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
    
    def test_dataset_analysis_asset(self):
        """Test dataset analysis calculation."""
        result = materialize_to_memory([random_dataset, dataset_stats, dataset_analysis])
        assert result.success
        analysis = result.output_for_node("dataset_analysis")
        assert isinstance(analysis, dict)
        assert "range" in analysis
        assert "above_average" in analysis
        assert "variance_indicator" in analysis


class TestStringProcessingAssets:
    """Test string processing assets functionality."""
    
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
    
    def test_message_summary_asset(self):
        """Test message summary functionality."""
        result = materialize_to_memory([raw_messages, processed_messages, message_summary])
        assert result.success
        summary = result.output_for_node("message_summary")
        assert isinstance(summary, dict)
        assert "total_messages" in summary
        assert "total_words" in summary
        assert summary["total_messages"] == 7


class TestWorkflowAssets:
    """Test workflow assets functionality."""
    
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


class TestExampleAssets:
    """Test example assets functionality."""
    
    def test_raw_data_asset(self):
        """Test raw data asset creation."""
        result = materialize_to_memory([raw_data])
        assert result.success
        data = result.output_for_node("raw_data")
        assert data == [1, 2, 3, 4, 5]
    
    def test_processed_data_asset(self):
        """Test processed data asset creation."""
        result = materialize_to_memory([raw_data, processed_data])
        assert result.success
        data = result.output_for_node("processed_data")
        assert data == [2, 4, 6, 8, 10]


class TestDefinitionsIntegration:
    """Test definitions-level integration."""
    
    def test_all_assets_can_materialize(self):
        """Test that all assets can be materialized together."""
        all_assets = [
            # Example assets
            raw_data, processed_data,
            # Basic math assets
            random_dataset, dataset_stats, dataset_analysis,
            # String processing assets
            raw_messages, processed_messages, message_summary,
            # Workflow assets
            workflow_metadata, execution_context, execution_report
        ]
        
        result = materialize_to_memory(all_assets)
        assert result.success
    
    def test_definitions_contains_all_assets(self):
        """Test that definitions contains all expected assets."""
        asset_names = [asset.key.to_user_string() for asset in defs.assets]
        
        expected_assets = [
            "raw_data", "processed_data",
            "random_dataset", "dataset_stats", "dataset_analysis",
            "raw_messages", "processed_messages", "message_summary",
            "workflow_metadata", "execution_context", "execution_report"
        ]
        
        for expected_asset in expected_assets:
            assert expected_asset in asset_names
        
        assert len(asset_names) == 11