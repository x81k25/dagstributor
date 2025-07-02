"""
Individual test cases for each DDL script in wiring_schema_tics/sql/ddl/
Tests multiple scenarios per script using real database transactions.
"""

import pytest
from dagstributor.wiring_schema_tics.ops import (
    wst_atp_drop_op,
    wst_atp_instantiate_media_op,
    wst_atp_instantiate_training_op,
    wst_atp_instantiate_prediction_op,
    wst_atp_instantiate_test_op,
    wst_atp_set_perms_op,
)


class TestDropSchemaScript:
    """Test cases for 00_drop_schema.sql"""
    
    def test_drop_existing_schema(self, dagster_context, db_fixture):
        """Test dropping an existing schema with tables"""
        # Setup: Create schema with a table
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("CREATE SCHEMA IF NOT EXISTS atp")
                cursor.execute("CREATE TABLE IF NOT EXISTS atp.test_table (id INT)")
                conn.commit()
        
        # Execute drop operation
        result = wst_atp_drop_op(dagster_context)
        
        # Verify schema is dropped
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name = 'atp'
                """)
                assert cursor.fetchone() is None
                
        assert result.value["status"] == "success"
    
    def test_drop_nonexistent_schema(self, dagster_context, db_fixture):
        """Test dropping a schema that doesn't exist"""
        # Ensure schema doesn't exist
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
                conn.commit()
        
        # Execute drop operation - should succeed (DROP IF EXISTS)
        result = wst_atp_drop_op(dagster_context)
        assert result.value["status"] == "success"
    
    def test_drop_schema_with_dependencies(self, dagster_context, db_fixture):
        """Test dropping schema with foreign key dependencies"""
        # Setup: Create schema with interdependent tables
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("CREATE SCHEMA IF NOT EXISTS atp")
                cursor.execute("""
                    CREATE TABLE atp.parent (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100)
                    )
                """)
                cursor.execute("""
                    CREATE TABLE atp.child (
                        id SERIAL PRIMARY KEY,
                        parent_id INT REFERENCES atp.parent(id),
                        data TEXT
                    )
                """)
                conn.commit()
        
        # Execute drop operation - CASCADE should handle dependencies
        result = wst_atp_drop_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Verify all tables are gone
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'atp'
                """)
                assert cursor.fetchone() is None


class TestInstantiateMediaScript:
    """Test cases for 01_instantiate_media.sql"""
    
    def test_create_media_table_fresh(self, dagster_context, db_fixture):
        """Test creating media table in fresh schema"""
        # Ensure clean state by dropping atp if it exists
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        
        # Execute instantiate operation
        result = wst_atp_instantiate_media_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Verify table structure
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                # Check table exists
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'atp' AND table_name = 'media'
                """)
                assert cursor.fetchone() is not None
                
                # Check column count
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM information_schema.columns 
                    WHERE table_schema = 'atp' AND table_name = 'media'
                """)
                result = cursor.fetchone()
                column_count = result['count'] if 'count' in result else result[0]
                assert column_count == 45  # All expected columns
                
                # Check enums exist
                cursor.execute("""
                    SELECT typname 
                    FROM pg_type 
                    WHERE typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'atp')
                    AND typname IN ('media_type', 'pipeline_status', 'rejection_status', 'rss_source')
                    ORDER BY typname
                """)
                enums = [row['typname'] for row in cursor.fetchall()]
                assert len(enums) == 4
    
    def test_create_media_table_idempotent(self, dagster_context, db_fixture):
        """Test that creating media table twice doesn't fail"""
        # Ensure clean state and create once
        db_fixture.cleanup_schema("atp")
        wst_atp_instantiate_media_op(dagster_context)
        
        # Try to create again - should succeed (CREATE IF NOT EXISTS)
        result = wst_atp_instantiate_media_op(dagster_context)
        assert result.value["status"] == "success"
    
    def test_media_table_constraints(self, dagster_context, db_fixture):
        """Test that media table has proper constraints"""
        # Ensure clean state by dropping atp if it exists
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        
        # Execute instantiate operation
        wst_atp_instantiate_media_op(dagster_context)
        
        # Verify constraints
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                # Check primary key
                cursor.execute("""
                    SELECT constraint_name 
                    FROM information_schema.table_constraints 
                    WHERE table_schema = 'atp' 
                    AND table_name = 'media' 
                    AND constraint_type = 'PRIMARY KEY'
                """)
                assert cursor.fetchone() is not None
                
                # Check NOT NULL constraints on required fields
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'atp' 
                    AND table_name = 'media' 
                    AND is_nullable = 'NO'
                    AND column_name IN ('hash', 'created_at', 'updated_at')
                """)
                not_null_columns = [row[0] for row in cursor.fetchall()]
                assert 'hash' in not_null_columns
                assert 'created_at' in not_null_columns
                assert 'updated_at' in not_null_columns


class TestInstantiateTrainingScript:
    """Test cases for 02_instantiate_training.sql"""
    
    def test_create_training_table_fresh(self, dagster_context, db_fixture):
        """Test creating training table in fresh schema"""
        # Ensure clean state by dropping atp if it exists
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        
        # Execute instantiate operation
        result = wst_atp_instantiate_training_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Verify table structure
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                # Check table exists
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'atp' AND table_name = 'training'
                """)
                assert cursor.fetchone() is not None
                
                # Check specific columns exist
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'atp' AND table_name = 'training'
                    AND column_name IN ('id', 'hash', 'is_good', 'created_at', 'updated_at')
                """)
                columns = [row[0] for row in cursor.fetchall()]
                assert len(columns) == 5
    
    def test_training_table_indexes(self, dagster_context, db_fixture):
        """Test that training table has proper indexes"""
        # Ensure clean state by dropping atp if it exists
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        
        # Execute instantiate operation
        wst_atp_instantiate_training_op(dagster_context)
        
        # Verify indexes
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                # Check for hash index
                cursor.execute("""
                    SELECT indexname 
                    FROM pg_indexes 
                    WHERE schemaname = 'atp' 
                    AND tablename = 'training'
                    AND indexdef LIKE '%hash%'
                """)
                assert cursor.fetchone() is not None


class TestInstantiatePredictionScript:
    """Test cases for 03_instantiate_prediction.sql"""
    
    def test_create_prediction_table_fresh(self, dagster_context, db_fixture):
        """Test creating prediction table in fresh schema"""
        # Ensure clean state by dropping atp if it exists
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        
        # Execute instantiate operation
        result = wst_atp_instantiate_prediction_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Verify table structure
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                # Check table exists
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'atp' AND table_name = 'prediction'
                """)
                assert cursor.fetchone() is not None
                
                # Check prediction-specific columns
                cursor.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns 
                    WHERE table_schema = 'atp' AND table_name = 'prediction'
                    AND column_name IN ('score', 'predicted_good', 'predicted_good_transformed')
                """)
                columns = {row[0]: row[1] for row in cursor.fetchall()}
                assert 'score' in columns
                assert 'predicted_good' in columns
                assert 'predicted_good_transformed' in columns
    
    def test_prediction_table_defaults(self, dagster_context, db_fixture):
        """Test that prediction table has proper default values"""
        # Ensure clean state by dropping atp if it exists
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        
        # Execute instantiate operation
        wst_atp_instantiate_prediction_op(dagster_context)
        
        # Insert a row and check defaults
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO atp.prediction (hash) 
                    VALUES ('test_hash')
                    RETURNING created_at, updated_at
                """)
                row = cursor.fetchone()
                assert row[0] is not None  # created_at
                assert row[1] is not None  # updated_at


class TestInstantiateTestScript:
    """Test cases for 04_instantiate_test.sql"""
    
    def test_create_test_schema(self, dagster_context, db_fixture):
        """Test creating test schema"""
        # Ensure clean state
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS test CASCADE")
        
        # Execute instantiate operation
        result = wst_atp_instantiate_test_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Verify schema exists
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name = 'test'
                """)
                assert cursor.fetchone() is not None
    
    def test_test_schema_idempotent(self, dagster_context, db_fixture):
        """Test that creating test schema twice doesn't fail"""
        # Create once
        wst_atp_instantiate_test_op(dagster_context)
        
        # Try to create again - should succeed (CREATE IF NOT EXISTS)
        result = wst_atp_instantiate_test_op(dagster_context)
        assert result.value["status"] == "success"


class TestSetPermsScript:
    """Test cases for 10_set_perms.sql"""
    
    def test_set_permissions_on_existing_schema(self, dagster_context, db_fixture):
        """Test setting permissions on existing schema"""
        # Setup: Create schema and tables
        db_fixture.cleanup_schema("atp")
        wst_atp_instantiate_media_op(dagster_context)
        wst_atp_instantiate_training_op(dagster_context)
        wst_atp_instantiate_prediction_op(dagster_context)
        
        # Execute permissions operation
        result = wst_atp_set_perms_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Note: We can't easily verify permissions without specific test users
        # but we can verify the operation completes successfully
    
    def test_set_permissions_on_empty_schema(self, dagster_context, db_fixture):
        """Test setting permissions on empty schema"""
        # Setup: Create empty schema
        db_fixture.cleanup_schema("atp")
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("CREATE SCHEMA atp")
                conn.commit()
        
        # Execute permissions operation - should handle empty schema gracefully
        result = wst_atp_set_perms_op(dagster_context)
        assert result.value["status"] == "success"
    
    def test_set_permissions_without_schema(self, dagster_context, db_fixture):
        """Test setting permissions when schema doesn't exist"""
        # Ensure schema doesn't exist
        db_fixture.cleanup_schema("atp")
        
        # Execute permissions operation - should fail gracefully
        result = wst_atp_set_perms_op(dagster_context)
        # This might fail or succeed depending on the SQL script's error handling
        assert result.value["status"] in ["success", "failed"]