"""Test operations for various testing purposes."""

import os
import time
import psycopg2
import psycopg2.extras
from dagster import op, Out, Output


@op(out=Out(dict))
def test_db_connection_op(context):
    """Test database connection with simple query."""
    context.log.info("Testing database connection")
    
    try:
        # Create database connection
        conn = psycopg2.connect(
            host=os.getenv('WST_PGSQL_HOST'),
            port=int(os.getenv('WST_PGSQL_PORT', 5432)),
            database=os.getenv('WST_PGSQL_DATABASE'),
            user=os.getenv('WST_PGSQL_USERNAME'),
            password=os.getenv('WST_PGSQL_PASSWORD'),
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        
        cursor = conn.cursor()
        
        # Execute simple test query
        test_query = "SELECT version() AS db_version, current_timestamp AS timestamp"
        context.log.info(f"Executing query: {test_query}")
        cursor.execute(test_query)
        
        # Fetch result
        result = cursor.fetchone()
        context.log.info(f"Database connection successful: {result['db_version']}")
        
        # Close connection
        cursor.close()
        conn.close()
        
        return Output(
            value={
                "status": "success",
                "db_version": result['db_version'],
                "timestamp": str(result['timestamp'])
            },
            metadata={
                "db_version": result['db_version'],
                "timestamp": str(result['timestamp']),
                "description": "Database connection test"
            }
        )
        
    except Exception as e:
        context.log.error(f"Failed to connect to database: {str(e)}")
        return Output(
            value={"status": "failed", "error": str(e)},
            metadata={"description": "Database connection test"}
        )


@op(out=Out(dict))
def test_timeout_conditions_op(context):
    """Test op that sleeps for 60 minutes to test timeout configurations."""
    context.log.info("Starting test_timeout_conditions op - will sleep for 60 minutes (3600 seconds)")
    
    try:
        # Sleep for 60 minutes
        time.sleep(3600)
        
        context.log.info("Test timeout conditions op completed successfully after 60 minutes")
        
        return Output(
            value={
                "status": "success",
                "sleep_duration_seconds": 3600,
                "sleep_duration_minutes": 60
            },
            metadata={
                "sleep_duration_seconds": 3600,
                "sleep_duration_minutes": 60,
                "test_purpose": "timeout_testing"
            }
        )
        
    except Exception as e:
        context.log.error(f"Test timeout conditions op failed: {str(e)}")
        return Output(
            value={"status": "failed", "error": str(e)},
            metadata={"test_purpose": "timeout_testing"}
        )