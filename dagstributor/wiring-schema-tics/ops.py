from dagster import op, Out, Output


@op(required_resource_keys={"postgres"}, out=Out(dict))
def test_db_connection_op(context):
    """Test database connection with a simple query."""
    conn = context.resources.postgres
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT * FROM atp.media;")
        results = cursor.fetchall()
        
        context.log.info(f"Successfully connected to database. Found {len(results)} records in atp.media")
        
        return Output(
            value={"status": "success", "record_count": len(results)},
            metadata={
                "record_count": len(results),
                "query": "SELECT * FROM atp.media"
            }
        )
    finally:
        cursor.close()