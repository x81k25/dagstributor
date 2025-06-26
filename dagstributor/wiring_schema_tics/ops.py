from dagster import op, Out, Output
from pathlib import Path


@op(required_resource_keys={"postgres"}, out=Out(dict))
def test_db_connection_op(context):
    """Test database connection by executing SQL from test.sql file."""
    # Read SQL from file
    sql_file = Path(__file__).parent / "sql" / "test.sql"
    
    try:
        with open(sql_file, 'r') as f:
            query = f.read().strip()
    except FileNotFoundError:
        context.log.error(f"SQL file not found: {sql_file}")
        raise
    
    conn = context.resources.postgres
    cursor = conn.cursor()
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        
        context.log.info(f"Successfully executed SQL from {sql_file.name}. Found {len(results)} records")
        
        return Output(
            value={"status": "success", "record_count": len(results), "sql_file": sql_file.name},
            metadata={
                "record_count": len(results),
                "sql_file": sql_file.name,
                "query": query
            }
        )
    finally:
        cursor.close()