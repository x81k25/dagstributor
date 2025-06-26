from dagster import resource
import psycopg2
from psycopg2.extras import RealDictCursor


@resource
def postgres_resource(context):
    """Resource for PostgreSQL database connections."""
    config = context.resource_config
    
    connection = psycopg2.connect(
        host=config["host"],
        port=int(config["port"]),
        database=config["database"],
        user=config["user"],
        password=config["password"],
        cursor_factory=RealDictCursor
    )
    
    try:
        yield connection
    finally:
        connection.close()