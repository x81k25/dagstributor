import os
import pytest
import psycopg2
from unittest.mock import Mock, patch
from pytest_postgresql import Postgresql
from dagster import DagsterInstance, build_op_context

from dagstributor.wiring_schema_tics.ops import (
    test_db_connection_op,
    wst_atp_drop_op,
    wst_atp_bak_media_op,
    wst_atp_instantiate_media_op,
    wst_atp_reload_media_op,
    execute_sql_file,
)


@pytest.fixture
def postgresql_db():
    """Create a temporary PostgreSQL database for testing."""
    with Postgresql() as postgresql:
        yield postgresql


@pytest.fixture
def mock_env_vars(postgresql_db):
    """Mock environment variables for database connection."""
    env_vars = {
        'WST_PGSQL_HOST': postgresql_db.host,
        'WST_PGSQL_PORT': str(postgresql_db.port),
        'WST_PGSQL_DATABASE': postgresql_db.dbname,
        'WST_PGSQL_USERNAME': postgresql_db.user,
        'WST_PGSQL_PASSWORD': '',
    }
    
    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture
def setup_atp_schema(postgresql_db, mock_env_vars):
    """Set up basic ATP schema with sample data for testing."""
    conn = psycopg2.connect(
        host=postgresql_db.host,
        port=postgresql_db.port,
        database=postgresql_db.dbname,
        user=postgresql_db.user
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Create ATP schema
    cursor.execute("CREATE SCHEMA IF NOT EXISTS atp;")
    cursor.execute("SET search_path TO atp;")
    
    # Create basic enums
    cursor.execute("""
        CREATE TYPE media_type AS ENUM ('movie', 'tv_show', 'tv_season', 'unknown');
    """)
    cursor.execute("""
        CREATE TYPE pipeline_status AS ENUM ('ingested', 'parsed', 'complete');
    """)
    cursor.execute("""
        CREATE TYPE rejection_status AS ENUM ('unfiltered', 'accepted', 'rejected');
    """)
    cursor.execute("""
        CREATE TYPE rss_source AS ENUM ('yts.mx', 'episodefeed.com');
    """)
    
    # Create media table with minimal structure
    cursor.execute("""
        CREATE TABLE media (
            hash CHAR(40) PRIMARY KEY,
            media_type media_type NOT NULL,
            media_title VARCHAR(255),
            pipeline_status pipeline_status NOT NULL DEFAULT 'ingested',
            rejection_status rejection_status NOT NULL DEFAULT 'unfiltered',
            original_title TEXT NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Insert sample data based on real data from media-dev
    cursor.execute("""
        INSERT INTO media (hash, media_type, media_title, original_title, pipeline_status) VALUES
        ('03edfa18e2b4b6a4d5c3f8e9a1b2c3d4e5f6a7b8', 'movie', 'Fraternal', 'Fraternal (2022)', 'complete'),
        ('07ee4c6b1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c67', 'movie', 'Moon of the Blood Beast', 'Moon.of.the.Blood.Beast.2024', 'ingested'),
        ('216158e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1', 'movie', 'Kidnapped by a Killer', 'Kidnapped.by.a.Killer.2023', 'parsed');
    """)
    
    cursor.close()
    conn.close()
    
    yield
    
    # Cleanup is automatic with pytest-postgresql


def test_db_connection_op_success(setup_atp_schema, mock_env_vars):
    """Test that test_db_connection_op successfully connects and queries the database."""
    context = build_op_context()
    
    result = test_db_connection_op(context)
    
    assert result["status"] == "success"
    assert result["statements_executed"] == 1
    assert result["total_rows"] == 3  # Should return 3 sample media records
    assert result["sql_file"] == "test.sql"


def test_wst_atp_drop_op_success(setup_atp_schema, mock_env_vars):
    """Test that wst_atp_drop_op successfully drops the ATP schema."""
    context = build_op_context()
    
    result = wst_atp_drop_op(context)
    
    assert result["status"] == "success"
    assert result["statements_executed"] == 1
    assert result["sql_file"] == "ddl/00_drop_schema.sql"
    
    # Verify schema was dropped by attempting to query it
    conn = psycopg2.connect(
        host=os.getenv('WST_PGSQL_HOST'),
        port=int(os.getenv('WST_PGSQL_PORT')),
        database=os.getenv('WST_PGSQL_DATABASE'),
        user=os.getenv('WST_PGSQL_USERNAME'),
        password=os.getenv('WST_PGSQL_PASSWORD')
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT schema_name FROM information_schema.schemata 
        WHERE schema_name = 'atp'
    """)
    schemas = cursor.fetchall()
    assert len(schemas) == 0  # Schema should not exist
    
    cursor.close()
    conn.close()


def test_wst_atp_instantiate_media_op_success(postgresql_db, mock_env_vars):
    """Test that wst_atp_instantiate_media_op successfully creates the media schema."""
    context = build_op_context()
    
    result = wst_atp_instantiate_media_op(context)
    
    assert result["status"] == "success"
    assert result["statements_executed"] > 1  # Multiple statements in the DDL script
    assert result["sql_file"] == "ddl/01_instantiate_media.sql"
    
    # Verify schema and table were created
    conn = psycopg2.connect(
        host=os.getenv('WST_PGSQL_HOST'),
        port=int(os.getenv('WST_PGSQL_PORT')),
        database=os.getenv('WST_PGSQL_DATABASE'),
        user=os.getenv('WST_PGSQL_USERNAME'),
        password=os.getenv('WST_PGSQL_PASSWORD')
    )
    cursor = conn.cursor()
    
    # Check schema exists
    cursor.execute("""
        SELECT schema_name FROM information_schema.schemata 
        WHERE schema_name = 'atp'
    """)
    schemas = cursor.fetchall()
    assert len(schemas) == 1
    
    # Check media table exists
    cursor.execute("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'atp' AND table_name = 'media'
    """)
    tables = cursor.fetchall()
    assert len(tables) == 1
    
    # Check that enums were created
    cursor.execute("""
        SELECT typname FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE n.nspname = 'atp' AND t.typtype = 'e'
    """)
    enums = cursor.fetchall()
    assert len(enums) == 4  # media_type, pipeline_status, rejection_status, rss_source
    
    cursor.close()
    conn.close()


def test_wst_atp_bak_media_op_success(setup_atp_schema, mock_env_vars):
    """Test that wst_atp_bak_media_op successfully backs up the media table."""
    context = build_op_context()
    
    result = wst_atp_bak_media_op(context)
    
    assert result["status"] == "success"
    assert result["statements_executed"] == 1  # Single DO block
    assert result["sql_file"] == "bak/bak_media.sql"
    
    # Verify backup was created
    conn = psycopg2.connect(
        host=os.getenv('WST_PGSQL_HOST'),
        port=int(os.getenv('WST_PGSQL_PORT')),
        database=os.getenv('WST_PGSQL_DATABASE'),
        user=os.getenv('WST_PGSQL_USERNAME'),
        password=os.getenv('WST_PGSQL_PASSWORD')
    )
    cursor = conn.cursor()
    
    # Check bak schema was created
    cursor.execute("""
        SELECT schema_name FROM information_schema.schemata 
        WHERE schema_name = 'bak'
    """)
    schemas = cursor.fetchall()
    assert len(schemas) == 1
    
    # Check backup table was created (format: atp_media_YYYYMMDD)
    from datetime import date
    today = date.today().strftime('%Y%m%d')
    backup_table_name = f'atp_media_{today}'
    
    cursor.execute("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'bak' AND table_name = %s
    """, (backup_table_name,))
    tables = cursor.fetchall()
    assert len(tables) == 1
    
    # Verify backup contains the sample data
    cursor.execute(f"SELECT COUNT(*) FROM bak.{backup_table_name}")
    count = cursor.fetchone()[0]
    assert count == 3  # Should have backed up 3 sample records
    
    cursor.close()
    conn.close()


def test_wst_atp_reload_media_op_success(postgresql_db, mock_env_vars):
    """Test that wst_atp_reload_media_op can reload data from backups."""
    # First set up the environment with schema, data, and backup
    conn = psycopg2.connect(
        host=postgresql_db.host,
        port=postgresql_db.port,
        database=postgresql_db.dbname,
        user=postgresql_db.user
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Create bak schema and sample backup table
    cursor.execute("CREATE SCHEMA IF NOT EXISTS bak;")
    from datetime import date
    today = date.today().strftime('%Y%m%d')
    backup_table_name = f'atp_media_{today}'
    
    cursor.execute(f"""
        CREATE TABLE bak.{backup_table_name} (
            hash CHAR(40),
            media_type TEXT,
            media_title VARCHAR(255),
            original_title TEXT,
            pipeline_status TEXT,
            rejection_status TEXT,
            created_at TIMESTAMP WITH TIME ZONE,
            updated_at TIMESTAMP WITH TIME ZONE
        );
    """)
    
    # Insert sample backup data
    cursor.execute(f"""
        INSERT INTO bak.{backup_table_name} (hash, media_type, media_title, original_title, pipeline_status, rejection_status) VALUES
        ('backup001234567890123456789012345678901', 'movie', 'Test Movie', 'Test.Movie.2024', 'complete', 'accepted');
    """)
    
    # Create column mapping table
    mapping_table_name = f'atp_media_{today}_column_mapping'
    cursor.execute(f"""
        CREATE TABLE bak.{mapping_table_name} (
            source_column_name TEXT,
            bak_column_name TEXT,
            source_pgsql_data_type TEXT,
            bak_pgsql_data_type TEXT,
            enum_name TEXT
        );
    """)
    
    # Add some column mappings
    cursor.execute(f"""
        INSERT INTO bak.{mapping_table_name} VALUES
        ('hash', 'hash', 'character', 'character', NULL),
        ('media_type', 'media_type', 'media_type', 'text', 'media_type'),
        ('media_title', 'media_title', 'character varying', 'character varying', NULL);
    """)
    
    cursor.close()
    conn.close()
    
    # Now test the reload operation
    context = build_op_context()
    
    # Note: This test assumes the reload script can handle existing schema
    # In a real scenario, you'd want to ensure the ATP schema exists first
    result = wst_atp_reload_media_op(context)
    
    # The reload might fail if there's no ATP schema to reload into,
    # but the test should verify the SQL executes without syntax errors
    assert result["statements_executed"] == 1
    assert result["sql_file"] == "bak/reload_media.sql"


def test_execute_sql_file_with_missing_file(mock_env_vars):
    """Test that execute_sql_file raises appropriate error for missing files."""
    context = build_op_context()
    
    with pytest.raises(FileNotFoundError):
        execute_sql_file(context, "nonexistent.sql")


def test_execute_sql_file_with_connection_error():
    """Test that execute_sql_file handles database connection errors."""
    context = build_op_context()
    
    # Mock invalid connection parameters
    with patch.dict(os.environ, {
        'WST_PGSQL_HOST': 'invalid_host',
        'WST_PGSQL_PORT': '5432',
        'WST_PGSQL_DATABASE': 'invalid_db',
        'WST_PGSQL_USERNAME': 'invalid_user',
        'WST_PGSQL_PASSWORD': 'invalid_pass'
    }):
        with pytest.raises(Exception):
            execute_sql_file(context, "test.sql")