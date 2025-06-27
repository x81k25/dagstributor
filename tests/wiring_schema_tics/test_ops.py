"""
Legacy test_ops.py - kept for compatibility.

Most comprehensive testing is now in test_all_sql_scripts.py.
This file contains basic smoke tests and backwards compatibility.
"""
import pytest
from dagster import build_op_context

from dagstributor.wiring_schema_tics.ops import execute_sql_file


def test_legacy_basic_connection(mock_env):
    """Legacy test for basic database connectivity."""
    # Import from conftest fixtures 
    pass  # Actual test moved to test_all_sql_scripts.py


def test_legacy_execute_sql_file_missing_file(mock_env):
    """Legacy test that execute_sql_file raises appropriate error for missing files."""
    context = build_op_context()
    
    with pytest.raises(FileNotFoundError):
        execute_sql_file(context, "nonexistent.sql")


def test_legacy_ops_are_importable():
    """Ensure all ops can be imported without errors."""
    from dagstributor.wiring_schema_tics.ops import (
        test_db_connection_op,
        wst_atp_drop_op,
        wst_atp_bak_media_op,
        wst_atp_bak_prediction_op,
        wst_atp_bak_training_op,
        wst_atp_instantiate_media_op,
        wst_atp_instantiate_training_op,
        wst_atp_instantiate_prediction_op,
        wst_atp_set_perms_op,
        wst_atp_reload_media_op,
        wst_atp_reload_training_op,
        wst_atp_reload_prediction_op,
    )
    
    # All ops should be callable (they're decorated functions)
    assert callable(test_db_connection_op)
    assert callable(wst_atp_drop_op)
    assert callable(wst_atp_bak_media_op)
    assert callable(wst_atp_instantiate_media_op)