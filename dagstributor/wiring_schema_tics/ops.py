from dagster import op, Out, Output
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


@op(out=Out(dict))
def test_db_connection_op(context):
    """Test database connection by executing SQL from test.sql file using enhanced psycopg2."""
    sql_filename = "test.sql"
    
    try:
        result = execute_sql_file(context, sql_filename)
        
        context.log.info(f"Executed {result['statements_executed']} statements, {result['total_rows']} total rows")
        
        # Create preview of results if any
        preview = ""
        if result['results']:
            preview = str(result['results'][:3])  # First 3 rows
            if len(result['results']) > 3:
                preview += f"... and {len(result['results']) - 3} more rows"
        
        return Output(
            value={
                "status": "success", 
                "statements_executed": result['statements_executed'],
                "total_rows": result['total_rows'],
                "sql_file": sql_filename,
                "results_preview": preview
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


@op(out=Out(dict))
def wst_atp_drop_op(context):
    """Drop the atp schema and all its objects. WARNING: This will delete all data!"""
    sql_filename = "ddl/00_drop_schema.sql"
    
    context.log.warning("⚠️  WARNING: This operation will DROP the atp schema and DELETE all data!")
    
    try:
        result = execute_sql_file(context, sql_filename)
        
        context.log.info(f"Successfully dropped schema. Executed {result['statements_executed']} statements")
        
        return Output(
            value={
                "status": "success",
                "statements_executed": result['statements_executed'],
                "sql_file": sql_filename,
                "message": "Schema 'atp' dropped successfully (if it existed)"
            },
            metadata={
                "statements_executed": result['statements_executed'],
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


@op(out=Out(dict))
def wst_atp_instantiate_op(context):
    """Execute DDL scripts to instantiate the atp schema in order."""
    ddl_scripts = [
        "ddl/01_instantiate_media.sql",
        "ddl/02_instantiate_training.sql", 
        "ddl/03_instantiate_prediction.sql",
        "ddl/10_set_perms.sql"
    ]
    
    results = []
    total_statements = 0
    total_rows = 0
    
    context.log.info("Starting schema instantiation with scripts: " + ", ".join(ddl_scripts))
    
    for script in ddl_scripts:
        try:
            context.log.info(f"Executing DDL script: {script}")
            result = execute_sql_file(context, script)
            
            results.append({
                "script": script,
                "status": "success",
                "statements_executed": result['statements_executed'],
                "total_rows": result['total_rows']
            })
            
            total_statements += result['statements_executed']
            total_rows += result['total_rows']
            
            context.log.info(f"Successfully executed {script}")
            
        except Exception as e:
            context.log.error(f"Failed to execute {script}: {str(e)}")
            results.append({
                "script": script,
                "status": "failed",
                "error": str(e)
            })
            
            # Stop execution on failure
            context.log.error("Stopping schema instantiation due to script failure")
            return Output(
                value={
                    "status": "failed",
                    "failed_script": script,
                    "scripts_executed": len([r for r in results if r['status'] == 'success']),
                    "total_scripts": len(ddl_scripts),
                    "error": str(e),
                    "results": results
                },
                metadata={
                    "failed_script": script,
                    "scripts_executed": len([r for r in results if r['status'] == 'success']),
                    "total_scripts": len(ddl_scripts)
                }
            )
    
    context.log.info(f"Schema instantiation completed successfully. Total: {total_statements} statements")
    
    return Output(
        value={
            "status": "success",
            "scripts_executed": len(results),
            "total_scripts": len(ddl_scripts),
            "total_statements": total_statements,
            "total_rows": total_rows,
            "results": results
        },
        metadata={
            "total_statements": total_statements,
            "total_rows": total_rows,
            "scripts_executed": len(results)
        }
    )


@op(out=Out(dict))
def wst_atp_reload_op(context):
    """Execute reload scripts to restore data from backup."""
    reload_scripts = [
        "bak/reload_media.sql",
        "bak/reload_training.sql",
        "bak/reload_prediction.sql"
    ]
    
    results = []
    total_statements = 0
    total_rows = 0
    
    context.log.info("Starting data reload with scripts: " + ", ".join(reload_scripts))
    
    for script in reload_scripts:
        try:
            context.log.info(f"Executing reload script: {script}")
            result = execute_sql_file(context, script)
            
            results.append({
                "script": script,
                "status": "success",
                "statements_executed": result['statements_executed'],
                "total_rows": result['total_rows']
            })
            
            total_statements += result['statements_executed']
            total_rows += result['total_rows']
            
            context.log.info(f"Successfully executed {script}")
            
        except Exception as e:
            context.log.error(f"Failed to execute {script}: {str(e)}")
            results.append({
                "script": script,
                "status": "failed",
                "error": str(e)
            })
            
            # Stop execution on failure
            context.log.error("Stopping data reload due to script failure")
            return Output(
                value={
                    "status": "failed",
                    "failed_script": script,
                    "scripts_executed": len([r for r in results if r['status'] == 'success']),
                    "total_scripts": len(reload_scripts),
                    "error": str(e),
                    "results": results
                },
                metadata={
                    "failed_script": script,
                    "scripts_executed": len([r for r in results if r['status'] == 'success']),
                    "total_scripts": len(reload_scripts)
                }
            )
    
    context.log.info(f"Data reload completed successfully. Total: {total_statements} statements, {total_rows} rows")
    
    return Output(
        value={
            "status": "success",
            "scripts_executed": len(results),
            "total_scripts": len(reload_scripts),
            "total_statements": total_statements,
            "total_rows": total_rows,
            "results": results
        },
        metadata={
            "total_statements": total_statements,
            "total_rows": total_rows,
            "scripts_executed": len(results)
        }
    )


@op(out=Out(dict))
def wst_atp_bak_op(context):
    """Execute all backup ATP scripts from sql/bak directory."""
    bak_scripts = ["bak_media.sql", "bak_prediction.sql", "bak_training.sql"]
    results = []
    total_statements = 0
    total_rows = 0
    
    for script in bak_scripts:
        try:
            context.log.info(f"Executing backup script: {script}")
            result = execute_sql_file(context, f"bak/{script}")
            
            results.append({
                "script": script,
                "status": "success",
                "statements_executed": result['statements_executed'],
                "total_rows": result['total_rows']
            })
            
            total_statements += result['statements_executed']
            total_rows += result['total_rows']
            
        except Exception as e:
            context.log.error(f"Failed to execute {script}: {str(e)}")
            results.append({
                "script": script,
                "status": "failed", 
                "error": str(e)
            })
    
    context.log.info(f"Completed backup ATP execution. Total: {total_statements} statements, {total_rows} rows")
    
    return Output(
        value={
            "status": "completed",
            "scripts_executed": len([r for r in results if r['status'] == 'success']),
            "total_scripts": len(bak_scripts),
            "total_statements": total_statements,
            "total_rows": total_rows,
            "results": results
        },
        metadata={
            "total_statements": total_statements,
            "total_rows": total_rows,
            "execution_method": "enhanced_psycopg2"
        }
    )