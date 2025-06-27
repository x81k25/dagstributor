"""
Shared pytest fixtures for wiring_schema_tics tests.
"""
import os
import pytest
from pathlib import Path
from unittest.mock import patch
from dagster import build_op_context

from .fixtures import DatabaseFixture, FixtureLoader


@pytest.fixture(scope="session")
def env_vars():
    """Load database connection parameters from local .env file."""
    env_file = Path(__file__).parent.parent.parent / ".env"
    env_vars = {}
    
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key] = value
    else:
        # Fallback values if .env doesn't exist
        env_vars = {
            'WST_PGSQL_HOST': '192.168.50.2',
            'WST_PGSQL_PORT': '31434',
            'WST_PGSQL_DATABASE': 'postgres',
            'WST_PGSQL_USERNAME': 'x81-test',
            'WST_PGSQL_PASSWORD': 'DB5jK2G7g8XZupdf6zFnWLsbTN',
        }
    
    return env_vars


@pytest.fixture
def mock_env(env_vars):
    """Mock environment variables for database connection."""
    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture
def dagster_context():
    """Create a Dagster op context for testing."""
    return build_op_context()


@pytest.fixture
def fixture_loader():
    """Create a FixtureLoader instance."""
    return FixtureLoader()


@pytest.fixture
def db_fixture(env_vars):
    """Create a DatabaseFixture instance."""
    return DatabaseFixture(env_vars)


@pytest.fixture
def test_schema(db_fixture):
    """Create a temporary test schema with full environment setup."""
    # Create unique test schema
    schema_name = db_fixture.create_test_schema()
    
    # Set up complete test environment
    setup_results = db_fixture.setup_full_test_environment(schema_name)
    
    yield {
        'schema_name': schema_name,
        'setup_results': setup_results,
        'db_fixture': db_fixture
    }
    
    # Cleanup: Drop the test schema
    db_fixture.drop_test_schema(schema_name)


@pytest.fixture
def empty_test_schema(db_fixture):
    """Create a temporary test schema without any tables."""
    schema_name = db_fixture.create_test_schema()
    
    yield {
        'schema_name': schema_name,
        'db_fixture': db_fixture
    }
    
    # Cleanup: Drop the test schema
    db_fixture.drop_test_schema(schema_name)


@pytest.fixture
def media_table_only(db_fixture):
    """Create a test schema with only the media table and sample data."""
    schema_name = db_fixture.create_test_schema()
    
    # Create just the media table
    db_fixture.create_table_from_schema(schema_name, "media")
    
    # Insert sample media data
    media_count = db_fixture.insert_sample_data(schema_name, "media", "media_samples")
    
    yield {
        'schema_name': schema_name,
        'media_count': media_count,
        'db_fixture': db_fixture
    }
    
    # Cleanup: Drop the test schema
    db_fixture.drop_test_schema(schema_name)


@pytest.fixture
def sample_data(fixture_loader):
    """Load sample data from JSON fixtures."""
    return fixture_loader.load_sample_data()


@pytest.fixture
def schemas_config(fixture_loader):
    """Load schema configurations from JSON fixtures."""
    return fixture_loader.load_schemas()


# Parametrized fixtures for testing different SQL scripts
@pytest.fixture(params=[
    "test.sql",
    "ddl/00_drop_schema.sql", 
    "ddl/01_instantiate_media.sql",
    "ddl/02_instantiate_training.sql", 
    "ddl/03_instantiate_prediction.sql",
    "ddl/10_set_perms.sql",
    "bak/bak_media.sql",
    "bak/bak_prediction.sql", 
    "bak/bak_training.sql",
    "bak/reload_media.sql",
    "bak/reload_prediction.sql",
    "bak/reload_training.sql"
])
def sql_script_path(request):
    """Parametrized fixture that yields each SQL script path."""
    return request.param


# Op fixtures for each SQL script type
@pytest.fixture
def test_ops():
    """Import and return all test ops for validation."""
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
    
    return {
        "test.sql": test_db_connection_op,
        "ddl/00_drop_schema.sql": wst_atp_drop_op,
        "ddl/01_instantiate_media.sql": wst_atp_instantiate_media_op,
        "ddl/02_instantiate_training.sql": wst_atp_instantiate_training_op,
        "ddl/03_instantiate_prediction.sql": wst_atp_instantiate_prediction_op,
        "ddl/10_set_perms.sql": wst_atp_set_perms_op,
        "bak/bak_media.sql": wst_atp_bak_media_op,
        "bak/bak_prediction.sql": wst_atp_bak_prediction_op,
        "bak/bak_training.sql": wst_atp_bak_training_op,
        "bak/reload_media.sql": wst_atp_reload_media_op,
        "bak/reload_prediction.sql": wst_atp_reload_prediction_op,
        "bak/reload_training.sql": wst_atp_reload_training_op,
    }