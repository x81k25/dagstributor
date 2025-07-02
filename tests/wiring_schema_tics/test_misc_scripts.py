"""
Individual test cases for miscellaneous SQL scripts (test.sql)
Tests multiple scenarios using real database transactions.
"""

import pytest
from dagstributor.wiring_schema_tics.ops import test_db_connection_op
import os
import psycopg2


class TestDatabaseConnectionScript:
    """Test cases for test.sql - database connection testing"""
    
    def test_basic_connection(self, dagster_context, db_fixture):
        """Test basic database connection and query execution"""
        # Setup: Create atp.media table since test.sql queries it
        from dagstributor.wiring_schema_tics.ops import wst_atp_drop_op, wst_atp_instantiate_media_op
        
        # Ensure clean state and create media table for test.sql
        wst_atp_drop_op(dagster_context)
        wst_atp_instantiate_media_op(dagster_context)
        
        # Execute test connection operation
        result = test_db_connection_op(dagster_context)
        
        # Verify success - test.sql queries atp.media which now exists
        assert result.value["status"] == "success"
        assert result.value["statements_executed"] == 1
        # total_rows will be 0 since atp.media is empty
        assert result.value["total_rows"] >= 0
    
    def test_connection_returns_correct_value(self, dagster_context, db_fixture):
        """Test that the test query executes successfully"""
        # Execute test connection operation
        result = test_db_connection_op(dagster_context)
        
        # The test.sql script queries atp.media - connection should succeed
        assert result.value["status"] == "success"
        
        # Verify by running a simple query directly to confirm DB connectivity
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 as test_value")
                result = cursor.fetchone()
                assert result['test_value'] == 1
    
    def test_connection_with_invalid_credentials(self, dagster_context, monkeypatch):
        """Test connection behavior with invalid database credentials"""
        # Temporarily set invalid password
        original_password = os.getenv('WST_PGSQL_PASSWORD')
        monkeypatch.setenv('WST_PGSQL_PASSWORD', 'invalid_password_12345')
        
        # Execute should fail gracefully
        result = test_db_connection_op(dagster_context)
        assert result.value["status"] == "failed"
        assert "error" in result.value
    
    def test_connection_with_wrong_database(self, dagster_context, monkeypatch):
        """Test connection behavior with non-existent database"""
        # Temporarily set invalid database
        monkeypatch.setenv('WST_PGSQL_DATABASE', 'nonexistent_db_12345')
        
        # Execute should fail gracefully
        result = test_db_connection_op(dagster_context)
        assert result.value["status"] == "failed"
        assert "error" in result.value
    
    def test_connection_with_wrong_host(self, dagster_context, monkeypatch):
        """Test connection behavior with unreachable host"""
        # Temporarily set invalid host
        monkeypatch.setenv('WST_PGSQL_HOST', 'invalid.host.that.does.not.exist')
        
        # Execute should fail gracefully
        result = test_db_connection_op(dagster_context)
        assert result.value["status"] == "failed"
        assert "error" in result.value
    
    def test_connection_with_wrong_port(self, dagster_context, monkeypatch):
        """Test connection behavior with wrong port"""
        # Temporarily set invalid port
        monkeypatch.setenv('WST_PGSQL_PORT', '9999')  # Unlikely to be correct
        
        # Execute should fail gracefully
        result = test_db_connection_op(dagster_context)
        assert result.value["status"] == "failed"
        assert "error" in result.value
    
    def test_multiple_sequential_connections(self, dagster_context, db_fixture):
        """Test multiple sequential connection tests"""
        # Execute multiple times to ensure connection pooling works
        results = []
        for i in range(5):
            result = test_db_connection_op(dagster_context)
            results.append(result)
        
        # All should succeed
        assert all(r.value["status"] == "success" for r in results)
        assert all(r.value["total_rows"] >= 0 for r in results)
    
    def test_connection_resilience(self, dagster_context, db_fixture):
        """Test that connection can recover after database operations"""
        # Create some database activity
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                # Create and drop a temporary table
                cursor.execute("CREATE TEMP TABLE test_temp (id INT)")
                cursor.execute("INSERT INTO test_temp VALUES (1), (2), (3)")
                cursor.execute("DROP TABLE test_temp")
                conn.commit()
        
        # Connection test should still work
        result = test_db_connection_op(dagster_context)
        assert result.value["status"] == "success"
    
    def test_connection_with_database_load(self, dagster_context, db_fixture):
        """Test connection while database is under load"""
        # Create some load with a complex query
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                # Generate a series to simulate load
                cursor.execute("""
                    SELECT COUNT(*) FROM (
                        SELECT generate_series(1, 10000) AS num
                    ) AS series
                """)
                cursor.fetchone()
        
        # Connection test should still work efficiently
        result = test_db_connection_op(dagster_context)
        assert result.value["status"] == "success"
        assert result.value["statements_executed"] == 1