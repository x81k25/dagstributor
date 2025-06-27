from dagster import op, Out, Output, job, execute_in_process
from pathlib import Path
import psycopg2
import psycopg2.extras
import os
import re


def execute_sql_file(context, sql_filename):
    """Execute SQL file using enhanced psycopg2 for robust handling of complex scripts."""
    sql_file = Path(__file__).parent / "sql" / sql_filename
    
    if not sql_file.exists():
        context.log.error(f"SQL file not found: {sql_file}")
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    
    context.log.info(f"Executing SQL file: {sql_filename}")
    
    # Read the entire SQL file
    try:
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
    except Exception as e:
        context.log.error(f"Failed to read SQL file {sql_filename}: {str(e)}")
        raise
    
    # Create database connection
    try:
        conn = psycopg2.connect(
            host=os.getenv('WST_PGSQL_HOST'),
            port=int(os.getenv('WST_PGSQL_PORT')),
            database=os.getenv('WST_PGSQL_DATABASE'),
            user=os.getenv('WST_PGSQL_USERNAME'),
            password=os.getenv('WST_PGSQL_PASSWORD'),
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        
        # Set autocommit for DDL operations and complex scripts
        conn.autocommit = True
        
    except Exception as e:
        context.log.error(f"Failed to connect to database: {str(e)}")
        raise
    
    cursor = conn.cursor()
    results = []
    executed_statements = 0
    
    try:
        # For complex SQL with dollar-quoted strings, execute as single block
        # Check if content has dollar-quoted strings
        if '$$' in sql_content:
            context.log.info("Detected dollar-quoted strings, executing as single block")
            statements = [sql_content.strip()]
        else:
            # Remove multi-line comment blocks (lines of dashes)
            # First, remove blocks that are just dashes and comments
            lines = sql_content.split('\n')
            cleaned_lines = []
            in_comment_block = False
            
            for line in lines:
                stripped = line.strip()
                # Check if this is a comment delimiter line (just dashes)
                if stripped and all(c == '-' for c in stripped):
                    in_comment_block = True
                    continue
                # If we're in a comment block and hit an empty line after comments, end the block
                elif in_comment_block and not stripped:
                    in_comment_block = False
                    continue
                # Skip comment lines when in a comment block
                elif in_comment_block and stripped.startswith('--'):
                    continue
                else:
                    in_comment_block = False
                    cleaned_lines.append(line)
            
            # Rejoin and split on semicolons
            cleaned_content = '\n'.join(cleaned_lines)
            statements = re.split(r';(?=(?:[^\']*\'[^\']*\')*[^\']*$)', cleaned_content)
            statements = [stmt.strip() for stmt in statements if stmt.strip()]
        
        context.log.info(f"Found {len(statements)} SQL statements to execute")
        
        for i, statement in enumerate(statements, 1):
            if not statement:
                continue
                
            context.log.debug(f"Executing statement {i}/{len(statements)}")
            
            try:
                cursor.execute(statement)
                executed_statements += 1
                
                # If it's a SELECT statement, fetch results
                if statement.strip().upper().startswith('SELECT'):
                    rows = cursor.fetchall()
                    results.extend(rows)
                    context.log.debug(f"Statement {i} returned {len(rows)} rows")
                else:
                    # For non-SELECT statements, log affected rows if available
                    if cursor.rowcount >= 0:
                        context.log.debug(f"Statement {i} affected {cursor.rowcount} rows")
                        
            except Exception as e:
                context.log.error(f"Failed executing statement {i}: {statement[:100]}...")
                context.log.error(f"Error: {str(e)}")
                raise Exception(f"SQL execution failed at statement {i}: {str(e)}")
        
        context.log.info(f"Successfully executed {executed_statements} statements from {sql_filename}")
        context.log.info(f"Total result rows: {len(results)}")
        
        return {
            'results': results,
            'statements_executed': executed_statements,
            'total_rows': len(results)
        }
        
    finally:
        cursor.close()
        conn.close()


def create_single_script_op(sql_filename, op_description):
    """Factory function to create a single-script SQL operation."""
    @op(out=Out(dict))
    def sql_op(context):
        try:
            result = execute_sql_file(context, sql_filename)
            
            context.log.info(f"{op_description} completed. Executed {result['statements_executed']} statements, {result['total_rows']} rows")
            
            return Output(
                value={
                    "status": "success",
                    "statements_executed": result['statements_executed'],
                    "total_rows": result['total_rows'],
                    "sql_file": sql_filename
                },
                metadata={
                    "statements_executed": result['statements_executed'],
                    "total_rows": result['total_rows'],
                    "sql_file": sql_filename,
                    "execution_method": "enhanced_psycopg2"
                }
            )
            
        except Exception as e:
            context.log.error(f"Failed to execute {sql_filename}: {str(e)}")
            return Output(
                value={"status": "failed", "error": str(e), "sql_file": sql_filename},
                metadata={"sql_file": sql_filename, "execution_method": "enhanced_psycopg2"}
            )
    
    return sql_op




# Individual script ops using factory function
test_db_connection_op = create_single_script_op("test.sql", "Database connection test")
wst_atp_drop_op = create_single_script_op("ddl/00_drop_schema.sql", "Schema drop")

# Backup ops
wst_atp_bak_media_op = create_single_script_op("bak/bak_media.sql", "Media backup")
wst_atp_bak_prediction_op = create_single_script_op("bak/bak_prediction.sql", "Prediction backup")
wst_atp_bak_training_op = create_single_script_op("bak/bak_training.sql", "Training backup")

# Instantiate ops
wst_atp_instantiate_media_op = create_single_script_op("ddl/01_instantiate_media.sql", "Media instantiation")
wst_atp_instantiate_training_op = create_single_script_op("ddl/02_instantiate_training.sql", "Training instantiation")
wst_atp_instantiate_prediction_op = create_single_script_op("ddl/03_instantiate_prediction.sql", "Prediction instantiation")
wst_atp_set_perms_op = create_single_script_op("ddl/10_set_perms.sql", "Permissions setting")

# Reload ops
wst_atp_reload_media_op = create_single_script_op("bak/reload_media.sql", "Media reload")
wst_atp_reload_training_op = create_single_script_op("bak/reload_training.sql", "Training reload")
wst_atp_reload_prediction_op = create_single_script_op("bak/reload_prediction.sql", "Prediction reload")


# Dynamic jobs for composite operations
@job
def backup_job():
    """Job that runs all backup operations simultaneously."""
    wst_atp_bak_media_op()
    wst_atp_bak_prediction_op()
    wst_atp_bak_training_op()


@job
def instantiate_job():
    """Job that runs instantiate operations sequentially."""
    media_result = wst_atp_instantiate_media_op()
    training_result = wst_atp_instantiate_training_op()
    prediction_result = wst_atp_instantiate_prediction_op()
    wst_atp_set_perms_op()


@job
def reload_job():
    """Job that runs all reload operations simultaneously."""
    wst_atp_reload_media_op()
    wst_atp_reload_training_op()
    wst_atp_reload_prediction_op()


@job
def bak_drop_reload_sequence_job():
    """Job that runs complete backup, drop, instantiate, and reload sequence."""
    # Backup
    media_bak = wst_atp_bak_media_op()
    prediction_bak = wst_atp_bak_prediction_op()
    training_bak = wst_atp_bak_training_op()
    
    # Drop (after backup completes)
    drop_result = wst_atp_drop_op()
    drop_result.add_dependencies([media_bak, prediction_bak, training_bak])
    
    # Instantiate (after drop completes)
    media_inst = wst_atp_instantiate_media_op()
    media_inst.add_dependency(drop_result)
    
    training_inst = wst_atp_instantiate_training_op()
    training_inst.add_dependency(media_inst)
    
    prediction_inst = wst_atp_instantiate_prediction_op()
    prediction_inst.add_dependency(training_inst)
    
    perms_result = wst_atp_set_perms_op()
    perms_result.add_dependency(prediction_inst)
    
    # Reload (after instantiate completes)
    media_reload = wst_atp_reload_media_op()
    media_reload.add_dependency(perms_result)
    
    training_reload = wst_atp_reload_training_op()
    training_reload.add_dependency(perms_result)
    
    prediction_reload = wst_atp_reload_prediction_op()
    prediction_reload.add_dependency(perms_result)


# Composite ops - these call jobs that run multiple ops
@op(out=Out(dict))
def wst_atp_bak_op(context):
    """Execute all backup operations simultaneously regardless of failure."""
    context.log.info("Starting backup operations via job execution")
    
    try:
        result = execute_in_process(backup_job)
        
        context.log.info(f"Backup job completed. Success: {result.success}")
        
        return Output(
            value={
                "status": "success" if result.success else "failed",
                "run_id": result.run_id,
                "success": result.success
            },
            metadata={
                "execution_method": "job_execution",
                "run_id": result.run_id,
                "success": result.success
            }
        )
        
    except Exception as e:
        context.log.error(f"Backup job execution failed: {str(e)}")
        return Output(
            value={"status": "failed", "error": str(e)},
            metadata={"execution_method": "job_execution"}
        )


@op(out=Out(dict))
def wst_atp_instantiate_op(context):
    """Execute instantiate operations sequentially, stop on first failure."""
    context.log.info("Starting instantiate operations via job execution")
    
    try:
        result = execute_in_process(instantiate_job)
        
        context.log.info(f"Instantiate job completed. Success: {result.success}")
        
        return Output(
            value={
                "status": "success" if result.success else "failed",
                "run_id": result.run_id,
                "success": result.success
            },
            metadata={
                "execution_method": "job_execution",
                "run_id": result.run_id,
                "success": result.success
            }
        )
        
    except Exception as e:
        context.log.error(f"Instantiate job execution failed: {str(e)}")
        return Output(
            value={"status": "failed", "error": str(e)},
            metadata={"execution_method": "job_execution"}
        )


@op(out=Out(dict))
def wst_atp_reload_op(context):
    """Execute all reload operations simultaneously regardless of failure."""
    context.log.info("Starting reload operations via job execution")
    
    try:
        result = execute_in_process(reload_job)
        
        context.log.info(f"Reload job completed. Success: {result.success}")
        
        return Output(
            value={
                "status": "success" if result.success else "failed",
                "run_id": result.run_id,
                "success": result.success
            },
            metadata={
                "execution_method": "job_execution",
                "run_id": result.run_id,
                "success": result.success
            }
        )
        
    except Exception as e:
        context.log.error(f"Reload job execution failed: {str(e)}")
        return Output(
            value={"status": "failed", "error": str(e)},
            metadata={"execution_method": "job_execution"}
        )


@op(out=Out(dict))
def wst_atp_bak_drop_reload_op(context):
    """Execute complete backup, drop, instantiate, and reload sequence using job execution."""
    context.log.info("Starting complete backup, drop, instantiate, and reload sequence via job execution")
    
    try:
        result = execute_in_process(bak_drop_reload_sequence_job)
        
        context.log.info(f"Full sequence job completed. Success: {result.success}")
        
        return Output(
            value={
                "status": "success" if result.success else "failed",
                "run_id": result.run_id,
                "success": result.success
            },
            metadata={
                "execution_method": "job_execution",
                "run_id": result.run_id,
                "success": result.success
            }
        )
        
    except Exception as e:
        context.log.error(f"Full sequence job execution failed: {str(e)}")
        return Output(
            value={"status": "failed", "error": str(e)},
            metadata={"execution_method": "job_execution"}
        )