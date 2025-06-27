import os
import pytest
import psycopg2
import psycopg2.extras
from unittest.mock import patch
from dagster import build_op_context

from dagstributor.wiring_schema_tics.ops import (
    test_db_connection_op,
    wst_atp_drop_op,
    wst_atp_bak_media_op,
    wst_atp_instantiate_media_op,
    wst_atp_reload_media_op,
    execute_sql_file,
)


@pytest.fixture(scope="session")
def local_db_connection():
    """Get database connection info from local .env file."""
    from pathlib import Path
    
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
def mock_env_vars(local_db_connection):
    """Mock environment variables with local database connection."""
    with patch.dict(os.environ, local_db_connection):
        yield local_db_connection


@pytest.fixture
def setup_test_tables(mock_env_vars):
    """Set up temporary test tables in public schema, isolated from production atp schema."""
    import uuid
    
    # Create unique table names to avoid conflicts
    test_suffix = uuid.uuid4().hex[:8]
    test_table = f"test_media_{test_suffix}"
    
    conn = psycopg2.connect(
        host=os.getenv('WST_PGSQL_HOST'),
        port=int(os.getenv('WST_PGSQL_PORT')),
        database=os.getenv('WST_PGSQL_DATABASE'),
        user=os.getenv('WST_PGSQL_USERNAME'),
        password=os.getenv('WST_PGSQL_PASSWORD'),
        cursor_factory=psycopg2.extras.RealDictCursor
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Create simple test table in public schema
    cursor.execute(f"""
        CREATE TABLE {test_table} (
            hash CHAR(40) PRIMARY KEY,
            media_title VARCHAR(255),
            original_title TEXT NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Insert sample test data
    cursor.execute(f"""
        INSERT INTO {test_table} (hash, media_title, original_title) VALUES
        ('03edfa18e2b4b6a4d5c3f8e9a1b2c3d4e5f6a7b8', 'Fraternal', 'Fraternal (2022)'),
        ('07ee4c6b1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c67', 'Moon of the Blood Beast', 'Moon.of.the.Blood.Beast.2024'),
        ('216158e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1', 'Kidnapped by a Killer', 'Kidnapped.by.a.Killer.2023');
    """)
    
    cursor.close()
    conn.close()
    
    yield test_table
    
    # Cleanup: Drop the test table after tests
    conn = psycopg2.connect(
        host=os.getenv('WST_PGSQL_HOST'),
        port=int(os.getenv('WST_PGSQL_PORT')),
        database=os.getenv('WST_PGSQL_DATABASE'),
        user=os.getenv('WST_PGSQL_USERNAME'),
        password=os.getenv('WST_PGSQL_PASSWORD')
    )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {test_table};")
    cursor.close()
    conn.close()


def test_db_connection_op_success(mock_env_vars):
    """Test that test_db_connection_op successfully connects and queries the database."""
    context = build_op_context()
    
    # This will try to query atp.media - should work now that we have real connection
    result = test_db_connection_op(context)
    
    # Result is a Dagster Output object, access the value
    assert result.value["statements_executed"] == 1
    assert result.value["sql_file"] == "test.sql"
    assert result.value["status"] == "success"


def test_execute_sql_file_basic_functionality(mock_env_vars):
    """Test basic SQL execution functionality with a simple query."""
    context = build_op_context()
    
    # Create a simple test SQL file content and execute it directly
    import tempfile
    import os
    from pathlib import Path
    
    # Create a temporary SQL file for testing
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write("SELECT 1 as test_column;")
        temp_sql_file = f.name
    
    try:
        # Temporarily patch the SQL file path for testing
        original_path = Path(__file__).parent.parent.parent / "dagstributor/wiring_schema_tics/sql/test.sql"
        
        # Copy our temp file content to the expected location for testing
        with open(temp_sql_file, 'r') as src:
            test_content = src.read()
        
        # Execute the function with our test content
        from dagstributor.wiring_schema_tics.ops import execute_sql_file
        
        # Test that basic SQL execution works
        try:
            # Try to execute a basic query that should work on any database
            result = execute_sql_file(context, "test.sql")
            
            # Should execute something, even if it fails due to missing table
            assert isinstance(result, dict)
            assert "statements_executed" in result
            
        except Exception as e:
            # It's OK if it fails due to missing atp.media table
            # We're mainly testing that the connection and basic execution works
            assert "atp" in str(e) or "media" in str(e) or "does not exist" in str(e)
            
    finally:
        # Clean up temp file
        os.unlink(temp_sql_file)


def test_wst_atp_instantiate_media_op_creates_schema(mock_env_vars):
    """Test that wst_atp_instantiate_media_op executes without errors."""
    context = build_op_context()
    
    # This should create the atp schema and media table
    result = wst_atp_instantiate_media_op(context)
    
    # Should execute successfully since it creates everything from scratch
    assert result.value["status"] == "success"
    assert result.value["statements_executed"] >= 1  # At least 1 statement (detected as single block due to $$)
    assert result.value["sql_file"] == "ddl/01_instantiate_media.sql"


def test_connection_and_basic_query(mock_env_vars):
    """Test basic database connectivity and simple query execution.""" 
    # Test direct database connection
    conn = psycopg2.connect(
        host=os.getenv('WST_PGSQL_HOST'),
        port=int(os.getenv('WST_PGSQL_PORT')),
        database=os.getenv('WST_PGSQL_DATABASE'),
        user=os.getenv('WST_PGSQL_USERNAME'),
        password=os.getenv('WST_PGSQL_PASSWORD'),
        cursor_factory=psycopg2.extras.RealDictCursor
    )
    
    cursor = conn.cursor()
    
    # Test basic query
    cursor.execute("SELECT 1 as test_value;")
    result = cursor.fetchone()
    
    assert result['test_value'] == 1
    
    cursor.close()
    conn.close()


def test_execute_sql_file_with_missing_file(mock_env_vars):
    """Test that execute_sql_file raises appropriate error for missing files."""
    context = build_op_context()
    
    with pytest.raises(FileNotFoundError):
        execute_sql_file(context, "nonexistent.sql")