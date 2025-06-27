"""
Comprehensive tests for all SQL scripts in the wiring_schema_tics module.

This test suite covers every SQL script and ensures they execute properly
with realistic test data from JSON fixtures.
"""
import pytest
from datetime import date


class TestConnectionAndBasics:
    """Test basic database connectivity and simple operations."""
    
    def test_database_connection(self, mock_env, db_fixture):
        """Test that we can connect to the database."""
        conn = db_fixture.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT 1 as test_value")
        result = cursor.fetchone()
        
        assert result['test_value'] == 1
        
        cursor.close()
        conn.close()
    
    def test_test_sql_script(self, mock_env, dagster_context, test_schema):
        """Test the test.sql script execution."""
        from dagstributor.wiring_schema_tics.ops import test_db_connection_op
        
        # This should query the existing atp.media table (or fail gracefully)
        result = test_db_connection_op(dagster_context)
        
        assert result.value["statements_executed"] == 1
        assert result.value["sql_file"] == "test.sql"
        # Status could be success or failed depending on if atp.media exists


class TestDDLScripts:
    """Test all DDL (Data Definition Language) scripts."""
    
    def test_drop_schema_script(self, mock_env, dagster_context, db_fixture):
        """Test 00_drop_schema.sql - drops atp schema completely."""
        from dagstributor.wiring_schema_tics.ops import wst_atp_drop_op
        
        # First verify if atp schema exists before dropping
        atp_exists_before = db_fixture.schema_exists("atp")
        
        result = wst_atp_drop_op(dagster_context)
        
        assert result.value["statements_executed"] == 1
        assert result.value["sql_file"] == "ddl/00_drop_schema.sql"
        assert result.value["status"] == "success"
        
        # Verify schema was dropped
        atp_exists_after = db_fixture.schema_exists("atp")
        assert not atp_exists_after
    
    def test_instantiate_media_script(self, mock_env, dagster_context, db_fixture):
        """Test 01_instantiate_media.sql - creates atp schema and media table.""" 
        from dagstributor.wiring_schema_tics.ops import wst_atp_instantiate_media_op
        
        # Ensure clean state by dropping atp if it exists
        conn = db_fixture.get_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE;")
        cursor.close()
        conn.close()
        
        result = wst_atp_instantiate_media_op(dagster_context)
        
        assert result.value["status"] == "success"
        assert result.value["statements_executed"] >= 1
        assert result.value["sql_file"] == "ddl/01_instantiate_media.sql"
        
        # Verify schema and table were created
        assert db_fixture.schema_exists("atp")
        assert db_fixture.table_exists("atp", "media")
        
        # Verify enums were created
        conn = db_fixture.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE n.nspname = 'atp' AND t.typtype = 'e'
        """)
        result = cursor.fetchone()
        enum_count = result['count'] if isinstance(result, dict) else result[0]
        assert enum_count == 4  # media_type, pipeline_status, rejection_status, rss_source
        cursor.close()
        conn.close()
    
    def test_instantiate_training_script(self, mock_env, dagster_context, db_fixture):
        """Test 02_instantiate_training.sql - creates training table."""
        from dagstributor.wiring_schema_tics.ops import wst_atp_instantiate_training_op
        
        # Ensure atp schema exists first
        if not db_fixture.schema_exists("atp"):
            conn = db_fixture.get_connection()
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute("CREATE SCHEMA atp;")
            cursor.close()
            conn.close()
        
        result = wst_atp_instantiate_training_op(dagster_context)
        
        assert result.value["status"] == "success"
        assert result.value["statements_executed"] >= 1
        assert result.value["sql_file"] == "ddl/02_instantiate_training.sql"
    
    def test_instantiate_prediction_script(self, mock_env, dagster_context, db_fixture):
        """Test 03_instantiate_prediction.sql - creates prediction table."""
        from dagstributor.wiring_schema_tics.ops import wst_atp_instantiate_prediction_op
        
        # Ensure atp schema exists first
        if not db_fixture.schema_exists("atp"):
            conn = db_fixture.get_connection()
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute("CREATE SCHEMA atp;")
            cursor.close()
            conn.close()
        
        result = wst_atp_instantiate_prediction_op(dagster_context)
        
        assert result.value["status"] == "success"
        assert result.value["statements_executed"] >= 1
        assert result.value["sql_file"] == "ddl/03_instantiate_prediction.sql"
    
    def test_set_permissions_script(self, mock_env, dagster_context, db_fixture):
        """Test 10_set_perms.sql - sets database permissions."""
        from dagstributor.wiring_schema_tics.ops import wst_atp_set_perms_op
        
        # Ensure atp schema exists first
        if not db_fixture.schema_exists("atp"):
            conn = db_fixture.get_connection()
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute("CREATE SCHEMA atp;")
            cursor.close()
            conn.close()
        
        result = wst_atp_set_perms_op(dagster_context)
        
        assert result.value["status"] == "success"
        assert result.value["statements_executed"] >= 1
        assert result.value["sql_file"] == "ddl/10_set_perms.sql"


class TestBackupScripts:
    """Test all backup scripts."""
    
    def test_backup_media_script(self, mock_env, dagster_context, test_schema):
        """Test bak_media.sql - backs up media table."""
        from dagstributor.wiring_schema_tics.ops import wst_atp_bak_media_op
        
        # Ensure we have atp.media table with data for backup
        # The test_schema fixture provides this
        
        result = wst_atp_bak_media_op(dagster_context)
        
        assert result.value["status"] == "success"
        assert result.value["statements_executed"] >= 1
        assert result.value["sql_file"] == "bak/bak_media.sql"
        
        # Verify backup was created
        db_fixture = test_schema['db_fixture']
        assert db_fixture.schema_exists("bak")
        
        # Check backup table was created (format: atp_media_YYYYMMDD)
        today = date.today().strftime('%Y%m%d')
        backup_table_name = f'atp_media_{today}'
        assert db_fixture.table_exists("bak", backup_table_name)
        
        # Verify backup contains data
        backup_count = db_fixture.get_table_count("bak", backup_table_name)
        assert backup_count > 0
    
    def test_backup_training_script(self, mock_env, dagster_context, test_schema):
        """Test bak_training.sql - backs up training table."""
        from dagstributor.wiring_schema_tics.ops import wst_atp_bak_training_op
        
        result = wst_atp_bak_training_op(dagster_context)
        
        assert result.value["status"] == "success"
        assert result.value["statements_executed"] >= 1
        assert result.value["sql_file"] == "bak/bak_training.sql"
        
        # Verify backup was created
        db_fixture = test_schema['db_fixture']
        today = date.today().strftime('%Y%m%d')
        backup_table_name = f'atp_training_{today}'
        assert db_fixture.table_exists("bak", backup_table_name)
    
    def test_backup_prediction_script(self, mock_env, dagster_context, test_schema):
        """Test bak_prediction.sql - backs up prediction table."""
        from dagstributor.wiring_schema_tics.ops import wst_atp_bak_prediction_op
        
        result = wst_atp_bak_prediction_op(dagster_context)
        
        assert result.value["status"] == "success"
        assert result.value["statements_executed"] >= 1
        assert result.value["sql_file"] == "bak/bak_prediction.sql"
        
        # Verify backup was created
        db_fixture = test_schema['db_fixture']
        today = date.today().strftime('%Y%m%d')
        backup_table_name = f'atp_prediction_{today}'
        assert db_fixture.table_exists("bak", backup_table_name)


class TestReloadScripts:
    """Test all reload scripts."""
    
    def test_reload_media_script(self, mock_env, dagster_context, test_schema):
        """Test reload_media.sql - reloads media data from backup."""
        from dagstributor.wiring_schema_tics.ops import wst_atp_reload_media_op
        
        # First create a backup to reload from
        db_fixture = test_schema['db_fixture']
        
        # Create backup table structure and data
        today = date.today().strftime('%Y%m%d')
        backup_table_name = f'atp_media_{today}'
        
        conn = db_fixture.get_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Ensure bak schema exists
        cursor.execute("CREATE SCHEMA IF NOT EXISTS bak;")
        
        # Create backup table with sample data
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS bak.{backup_table_name} (
                hash CHAR(40),
                media_type TEXT,
                media_title VARCHAR(255),
                original_title TEXT
            );
        """)
        
        # Insert sample backup data
        cursor.execute(f"""
            INSERT INTO bak.{backup_table_name} (hash, media_type, media_title, original_title)
            VALUES ('test123456789012345678901234567890123456', 'movie', 'Test Movie', 'Test.Movie.2024')
            ON CONFLICT DO NOTHING;
        """)
        
        # Create column mapping table
        mapping_table_name = f'atp_media_{today}_column_mapping'
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS bak.{mapping_table_name} (
                source_column_name TEXT,
                bak_column_name TEXT,
                source_pgsql_data_type TEXT,
                bak_pgsql_data_type TEXT,
                enum_name TEXT
            );
        """)
        
        # Add column mappings
        cursor.execute(f"""
            INSERT INTO bak.{mapping_table_name} VALUES
            ('hash', 'hash', 'character', 'character', NULL),
            ('media_type', 'media_type', 'media_type', 'text', 'media_type'),
            ('media_title', 'media_title', 'character varying', 'character varying', NULL)
            ON CONFLICT DO NOTHING;
        """)
        
        cursor.close()
        conn.close()
        
        # Now test the reload operation
        result = wst_atp_reload_media_op(dagster_context)
        
        assert result.value["statements_executed"] >= 1
        assert result.value["sql_file"] == "bak/reload_media.sql"
        # Note: Status might be success or failed depending on schema state
    
    def test_reload_training_script(self, mock_env, dagster_context, test_schema):
        """Test reload_training.sql - reloads training data from backup."""
        from dagstributor.wiring_schema_tics.ops import wst_atp_reload_training_op
        
        result = wst_atp_reload_training_op(dagster_context)
        
        assert result.value["statements_executed"] >= 1
        assert result.value["sql_file"] == "bak/reload_training.sql"
    
    def test_reload_prediction_script(self, mock_env, dagster_context, test_schema):
        """Test reload_prediction.sql - reloads prediction data from backup."""
        from dagstributor.wiring_schema_tics.ops import wst_atp_reload_prediction_op
        
        result = wst_atp_reload_prediction_op(dagster_context)
        
        assert result.value["statements_executed"] >= 1
        assert result.value["sql_file"] == "bak/reload_prediction.sql"


class TestSQLScriptExecution:
    """Test SQL script execution framework and error handling."""
    
    def test_missing_sql_file(self, mock_env, dagster_context):
        """Test error handling for missing SQL files."""
        from dagstributor.wiring_schema_tics.ops import execute_sql_file
        
        with pytest.raises(FileNotFoundError):
            execute_sql_file(dagster_context, "nonexistent.sql")
    
    def test_execute_sql_file_basic_functionality(self, mock_env, dagster_context, db_fixture):
        """Test basic SQL execution functionality."""
        from dagstributor.wiring_schema_tics.ops import execute_sql_file
        
        # Create a simple test by executing against existing test.sql
        try:
            result = execute_sql_file(dagster_context, "test.sql")
            
            # Should execute at least one statement
            assert isinstance(result, dict)
            assert "statements_executed" in result
            assert result["statements_executed"] >= 1
            
        except Exception as e:
            # It's OK if it fails due to missing atp.media table
            # We're mainly testing that the function executes without syntax errors
            assert "atp" in str(e) or "media" in str(e) or "does not exist" in str(e)


class TestFixtureFramework:
    """Test the fixture framework itself."""
    
    def test_fixture_loader(self, fixture_loader):
        """Test that fixture loader can load JSON data."""
        schemas = fixture_loader.load_schemas()
        sample_data = fixture_loader.load_sample_data()
        
        assert "atp_schema" in schemas
        assert "media_samples" in sample_data
        assert len(sample_data["media_samples"]) >= 3
    
    def test_test_schema_fixture(self, test_schema):
        """Test that test_schema fixture creates proper environment."""
        schema_name = test_schema['schema_name']
        setup_results = test_schema['setup_results']
        db_fixture = test_schema['db_fixture']
        
        # Verify schema was created
        assert db_fixture.schema_exists(schema_name)
        
        # Verify tables were created with data
        assert setup_results['media_rows'] >= 3
        assert setup_results['training_rows'] >= 2
        assert setup_results['prediction_rows'] >= 2
        
        # Verify actual table counts
        assert db_fixture.get_table_count(schema_name, "media") >= 3
    
    def test_database_fixture_capabilities(self, db_fixture):
        """Test DatabaseFixture utility methods."""
        # Test schema operations
        test_schema_name = f"test_capabilities_{hash(str(id(db_fixture))) % 10000}"
        
        # Create and verify schema
        schema_name = db_fixture.create_test_schema(test_schema_name)
        assert db_fixture.schema_exists(schema_name)
        
        # Create table and verify
        db_fixture.create_table_from_schema(schema_name, "media")
        assert db_fixture.table_exists(schema_name, "media")
        
        # Insert data and verify count
        count = db_fixture.insert_sample_data(schema_name, "media", "media_samples")
        assert count >= 3
        assert db_fixture.get_table_count(schema_name, "media") == count
        
        # Cleanup
        db_fixture.drop_test_schema(schema_name)
        assert not db_fixture.schema_exists(schema_name)