from dagster import op, Out, Output
from pathlib import Path
import subprocess
import os


def execute_sql_file(context, sql_filename):
    """Execute SQL file using psql subprocess for robust handling of complex scripts."""
    sql_file = Path(__file__).parent / "sql" / sql_filename
    
    if not sql_file.exists():
        context.log.error(f"SQL file not found: {sql_file}")
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    
    context.log.info(f"Executing SQL file: {sql_filename}")
    
    # Use psql subprocess for robust SQL execution
    result = subprocess.run([
        'psql',
        '-h', os.getenv('WST_PGSQL_HOST'),
        '-p', os.getenv('WST_PGSQL_PORT'), 
        '-U', os.getenv('WST_PGSQL_USERNAME'),
        '-d', os.getenv('WST_PGSQL_DATABASE'),
        '-f', str(sql_file),
        '--quiet',  # reduce noise
        '--no-align',  # clean output
        '--tuples-only'  # data only
    ], 
    env={
        **os.environ,
        'PGPASSWORD': os.getenv('WST_PGSQL_PASSWORD')
    },
    capture_output=True, 
    text=True,
    timeout=3600  # 1 hour timeout
    )
    
    if result.returncode != 0:
        context.log.error(f"SQL execution failed for {sql_filename}: {result.stderr}")
        raise Exception(f"SQL execution failed for {sql_filename}: {result.stderr}")
    
    context.log.info(f"Successfully executed SQL file: {sql_filename}")
    return result.stdout


@op(out=Out(dict))
def test_db_connection_op(context):
    """Test database connection by executing SQL from test.sql file using robust psql execution."""
    sql_filename = "test.sql"
    
    try:
        output = execute_sql_file(context, sql_filename)
        
        # Count lines in output to estimate record count
        lines = output.strip().split('\n') if output.strip() else []
        record_count = len([line for line in lines if line.strip()])
        
        context.log.info(f"Found {record_count} records from {sql_filename}")
        
        return Output(
            value={
                "status": "success", 
                "record_count": record_count, 
                "sql_file": sql_filename,
                "output_preview": output[:500] if output else "No output"
            },
            metadata={
                "record_count": record_count,
                "sql_file": sql_filename,
                "execution_method": "psql_subprocess"
            }
        )
        
    except Exception as e:
        context.log.error(f"Failed to execute {sql_filename}: {str(e)}")
        return Output(
            value={"status": "failed", "error": str(e), "sql_file": sql_filename},
            metadata={"sql_file": sql_filename, "execution_method": "psql_subprocess"}
        )