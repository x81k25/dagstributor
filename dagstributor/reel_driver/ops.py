"""Kubernetes container execution ops for reel-driver pipeline."""

import os
from dagster import op, OpExecutionContext, Out, Output
from dagster_k8s import k8s_job_op
from pathlib import Path
import psycopg2
import psycopg2.extras
import re


def get_environment():
    """Get and validate ENVIRONMENT variable. Fails if not set."""
    env = os.environ.get('ENVIRONMENT')
    if not env:
        raise ValueError(
            "ENVIRONMENT variable is not set. "
            "This must be set to 'dev', 'stg', or 'prod'."
        )
    if env not in ['dev', 'stg', 'prod']:
        raise ValueError(
            f"Invalid ENVIRONMENT value: '{env}'. "
            f"Must be one of: 'dev', 'stg', 'prod'."
        )
    return env


# Image tag logic: prod environment uses 'main' tags, others use environment name
def get_image_tag():
    env = get_environment()
    return 'main' if env == 'prod' else env


# Global K8s job configuration for CPU ML workloads
def get_base_k8s_config_cpu():
    env = get_environment()
    return {
        "namespace": "ai-ml",
        "service_account_name": "default",
        "image_pull_secrets": [{"name": "ghcr-pull-image-secret"}],
        "env_config_maps": [
            f"reel-driver-config-{env}",
            f"reel-driver-training-config-{env}"
        ],
        "env_secrets": [
            f"reel-driver-secrets-{env}",
            f"reel-driver-training-secrets-{env}"
        ],
        "container_config": {
            "resources": {
                "limits": {
                    "cpu": "8",
                    "memory": "8Gi"
                },
                "requests": {
                    "cpu": "2",
                    "memory": "2Gi"
                }
            },
            "env": [
                {"name": "ENVIRONMENT", "value": env}
            ]
        },
        "job_spec_config": {
            "activeDeadlineSeconds": 43200,  # 12 hours timeout for ML workloads
            "backoffLimit": 1  # Allow 1 retry for transient failures
        },
    }


# Global K8s job configuration for GPU ML workloads (RTX 3060)
def get_base_k8s_config_gpu():
    env = get_environment()
    return {
        "namespace": "ai-ml",
        "service_account_name": "default",
        "image_pull_secrets": [{"name": "ghcr-pull-image-secret"}],
        "env_config_maps": [
            f"reel-driver-config-{env}",
            f"reel-driver-training-config-{env}"
        ],
        "env_secrets": [
            f"reel-driver-secrets-{env}",
            f"reel-driver-training-secrets-{env}"
        ],
        "container_config": {
            "resources": {
                "limits": {
                    "cpu": "4",
                    "memory": "8Gi",
                    "nvidia.com/gpu": "1"
                },
                "requests": {
                    "cpu": "2",
                    "memory": "2Gi",
                    "nvidia.com/gpu": "1"
                }
            },
            "env": [
                {"name": "ENVIRONMENT", "value": env},
                {"name": "NVIDIA_VISIBLE_DEVICES", "value": "GPU-cfbe0295-2bfb-12c9-1bc9-b3b4833f2e18"}
            ]
        },
        "pod_spec_config": {
            "runtime_class_name": "nvidia"
        },
        "job_spec_config": {
            "activeDeadlineSeconds": 43200,  # 12 hours timeout for ML workloads
            "backoffLimit": 1  # Allow 1 retry for transient failures
        },
    }


# CPU ops
reel_driver_feature_engineering_cpu_op = k8s_job_op.configured(
    {
        **get_base_k8s_config_cpu(),
        "image": f"ghcr.io/x81k25/reel-driver/reel-driver-feature-engineering-cpu:{get_image_tag()}",
        "container_config": {
            **get_base_k8s_config_cpu()["container_config"],
            "name": "reel-driver-feature-engineering-cpu"
        }
    },
    name="reel_driver_feature_engineering_cpu_op"
)

reel_driver_model_training_cpu_op = k8s_job_op.configured(
    {
        **get_base_k8s_config_cpu(),
        "image": f"ghcr.io/x81k25/reel-driver/reel-driver-model-training-cpu:{get_image_tag()}",
        "container_config": {
            **get_base_k8s_config_cpu()["container_config"],
            "name": "reel-driver-model-training-cpu"
        }
    },
    name="reel_driver_model_training_cpu_op"
)

# GPU ops (RTX 3060)
reel_driver_feature_engineering_gpu_op = k8s_job_op.configured(
    {
        **get_base_k8s_config_gpu(),
        "image": f"ghcr.io/x81k25/reel-driver/reel-driver-feature-engineering-gpu:{get_image_tag()}",
        "container_config": {
            **get_base_k8s_config_gpu()["container_config"],
            "name": "reel-driver-feature-engineering-gpu"
        }
    },
    name="reel_driver_feature_engineering_gpu_op"
)

reel_driver_model_training_gpu_op = k8s_job_op.configured(
    {
        **get_base_k8s_config_gpu(),
        "image": f"ghcr.io/x81k25/reel-driver/reel-driver-model-training-gpu:{get_image_tag()}",
        "container_config": {
            **get_base_k8s_config_gpu()["container_config"],
            "name": "reel-driver-model-training-gpu"
        }
    },
    name="reel_driver_model_training_gpu_op"
)


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
    # Generate unique name from filename
    op_name = sql_filename.replace("/", "_").replace(".sql", "_op")
    
    @op(out=Out(dict), name=op_name)
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


# Review all op
reel_driver_review_all_op = create_single_script_op("review_all.sql", "Review all reset")