from dagster import resource
import psycopg2
import os
from psycopg2.extras import RealDictCursor


@resource
def postgres_resource(context):
    """Resource for PostgreSQL database connections."""
    
    connection = psycopg2.connect(
        host=os.getenv("WST_PGSQL_HOST"),
        port=int(os.getenv("WST_PGSQL_PORT")),
        database=os.getenv("WST_PGSQL_DATABASE"),
        user=os.getenv("WST_PGSQL_USERNAME"),
        password=os.getenv("WST_PGSQL_PASSWORD"),
        cursor_factory=RealDictCursor
    )
    
    try:
        yield connection
    finally:
        connection.close()