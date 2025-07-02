"""
Fixture utilities for loading JSON test data and creating database structures.
"""
import json
import uuid
from pathlib import Path
from typing import Dict, List, Any, Optional
import psycopg2
import psycopg2.extras


class FixtureLoader:
    """Utility class for loading and working with JSON fixtures."""
    
    def __init__(self, fixtures_dir: Optional[Path] = None):
        if fixtures_dir is None:
            fixtures_dir = Path(__file__).parent / "fixtures"
        self.fixtures_dir = fixtures_dir
        
    def load_schemas(self) -> Dict[str, Any]:
        """Load schema definitions from schemas.json."""
        schema_file = self.fixtures_dir / "schemas.json"
        with open(schema_file, 'r') as f:
            return json.load(f)
    
    def load_sample_data(self) -> Dict[str, Any]:
        """Load sample data from sample_data.json."""
        data_file = self.fixtures_dir / "sample_data.json"
        with open(data_file, 'r') as f:
            return json.load(f)
    
    def load_backup_fixtures(self) -> Dict[str, Any]:
        """Load backup fixtures from backup_fixtures.json."""
        backup_file = self.fixtures_dir / "backup_fixtures.json"
        with open(backup_file, 'r') as f:
            return json.load(f)
    
    def get_table_schema(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """Get schema definition for a specific table."""
        schemas = self.load_schemas()
        return schemas[schema_name]["tables"][table_name]
    
    def get_enum_definitions(self, schema_name: str) -> List[Dict[str, Any]]:
        """Get enum definitions for a schema."""
        schemas = self.load_schemas()
        return schemas[schema_name].get("enums", [])


class DatabaseFixture:
    """Utility class for creating test database structures."""
    
    def __init__(self, connection_params: Dict[str, Any]):
        self.connection_params = connection_params
        self.loader = FixtureLoader()
        
    def get_connection(self):
        """Get database connection."""
        return psycopg2.connect(
            host=self.connection_params['WST_PGSQL_HOST'],
            port=int(self.connection_params['WST_PGSQL_PORT']),
            database=self.connection_params['WST_PGSQL_DATABASE'],
            user=self.connection_params['WST_PGSQL_USERNAME'],
            password=self.connection_params['WST_PGSQL_PASSWORD'],
            cursor_factory=psycopg2.extras.RealDictCursor
        )
    
    def create_test_schema(self, base_name: str = "test_atp") -> str:
        """Create a unique test schema and return its name."""
        schema_name = f"{base_name}_{uuid.uuid4().hex[:8]}"
        
        conn = self.get_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"CREATE SCHEMA {schema_name};")
            cursor.execute(f"SET search_path TO {schema_name};")
            
            # Create enums
            enums = self.loader.get_enum_definitions("atp_schema")
            for enum_def in enums:
                enum_values = "', '".join(enum_def["values"])
                cursor.execute(f"""
                    CREATE TYPE {enum_def["name"]} AS ENUM ('{enum_values}');
                """)
            
            return schema_name
            
        finally:
            cursor.close()
            conn.close()
    
    def create_table_from_schema(self, schema_name: str, table_name: str, target_schema: str = "atp") -> None:
        """Create a table in the test schema based on JSON schema definition."""
        table_schema = self.loader.get_table_schema("atp_schema", table_name)
        
        conn = self.get_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"SET search_path TO {schema_name};")
            
            # Build CREATE TABLE statement
            columns = []
            for col in table_schema["columns"]:
                col_def = f"{col['name']} {col['type']}"
                
                # Add constraints
                if col.get("constraints"):
                    col_def += " " + " ".join(col["constraints"])
                
                # Add default
                if col.get("default"):
                    col_def += f" DEFAULT {col['default']}"
                
                # Add check constraints
                if col.get("check"):
                    col_def += f" CHECK ({col['check']})"
                
                columns.append(col_def)
            
            create_sql = f"""
                CREATE TABLE {table_name} (
                    {', '.join(columns)}
                );
            """
            
            cursor.execute(create_sql)
            
            # Create indexes if defined
            if "indexes" in table_schema:
                for index in table_schema["indexes"]:
                    index_cols = ", ".join(index["columns"])
                    cursor.execute(f"""
                        CREATE INDEX {index["name"]} ON {table_name}({index_cols});
                    """)
            
        finally:
            cursor.close()
            conn.close()
    
    def insert_sample_data(self, schema_name: str, table_name: str, data_key: str) -> int:
        """Insert sample data into a table and return number of rows inserted."""
        sample_data = self.loader.load_sample_data()
        records = sample_data.get(data_key, [])
        
        if not records:
            return 0
        
        conn = self.get_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"SET search_path TO {schema_name};")
            
            # Get column names from first record
            columns = list(records[0].keys())
            placeholders = ", ".join(["%s"] * len(columns))
            column_names = ", ".join(columns)
            
            insert_sql = f"""
                INSERT INTO {table_name} ({column_names}) 
                VALUES ({placeholders})
            """
            
            # Prepare data for insertion
            rows = []
            for record in records:
                row = []
                for col in columns:
                    value = record[col]
                    # Convert Python None to SQL NULL
                    if value is None:
                        row.append(None)
                    # Convert Python lists to PostgreSQL arrays
                    elif isinstance(value, list):
                        row.append(value)
                    # Convert Python dicts to JSON
                    elif isinstance(value, dict):
                        row.append(json.dumps(value))
                    else:
                        row.append(value)
                rows.append(row)
            
            cursor.executemany(insert_sql, rows)
            return len(rows)
            
        finally:
            cursor.close()
            conn.close()
    
    def drop_test_schema(self, schema_name: str) -> None:
        """Drop a test schema and all its contents."""
        conn = self.get_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
        finally:
            cursor.close()
            conn.close()
    
    def setup_full_test_environment(self, schema_name: str) -> Dict[str, int]:
        """Set up a complete test environment with all tables and sample data."""
        results = {}
        
        # Create tables
        tables = ["media", "training", "prediction"]
        for table in tables:
            self.create_table_from_schema(schema_name, table)
        
        # Insert sample data
        data_mappings = {
            "media": "media_samples",
            "training": "training_samples", 
            "prediction": "prediction_samples"
        }
        
        for table, data_key in data_mappings.items():
            count = self.insert_sample_data(schema_name, table, data_key)
            results[f"{table}_rows"] = count
        
        return results
    
    def get_table_count(self, schema_name: str, table_name: str) -> int:
        """Get the number of rows in a table."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
            result = cursor.fetchone()
            return result['count'] if isinstance(result, dict) else result[0]
        finally:
            cursor.close()
            conn.close()
    
    def table_exists(self, schema_name: str, table_name: str) -> bool:
        """Check if a table exists in the given schema."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                )
            """, (schema_name, table_name))
            result = cursor.fetchone()
            return result['exists'] if isinstance(result, dict) else result[0]
        finally:
            cursor.close()
            conn.close()
    
    def schema_exists(self, schema_name: str) -> bool:
        """Check if a schema exists."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.schemata 
                    WHERE schema_name = %s
                )
            """, (schema_name,))
            result = cursor.fetchone()
            return result['exists'] if isinstance(result, dict) else result[0]
        finally:
            cursor.close()
            conn.close()
    
    def create_backup_environment(self, table_type: str, date_str: Optional[str] = None) -> Dict[str, str]:
        """Create backup tables and column mappings for testing reload operations."""
        from datetime import date
        
        if date_str is None:
            date_str = date.today().strftime('%Y%m%d')
        
        backup_fixtures = self.loader.load_backup_fixtures()
        
        # Get backup table structure
        backup_key = f"{table_type}_backup"
        if backup_key not in backup_fixtures["backup_table_structures"]:
            raise ValueError(f"Unknown table type: {table_type}")
        
        backup_structure = backup_fixtures["backup_table_structures"][backup_key]
        mapping_structure = backup_fixtures["column_mapping_structures"][f"{table_type}_column_mapping"]
        
        # Generate table names
        backup_table_name = backup_structure["table_name_pattern"].format(date=date_str)
        mapping_table_name = mapping_structure["table_name_pattern"].format(date=date_str)
        
        conn = self.get_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        
        try:
            # Ensure bak schema exists
            cursor.execute("CREATE SCHEMA IF NOT EXISTS bak;")
            
            # Create backup table
            backup_columns = []
            for col in backup_structure["columns"]:
                backup_columns.append(f"{col['name']} {col['type']}")
            
            backup_sql = f"""
                CREATE TABLE IF NOT EXISTS bak.{backup_table_name} (
                    {', '.join(backup_columns)}
                );
            """
            cursor.execute(backup_sql)
            
            # Create column mapping table  
            mapping_columns = []
            for col in mapping_structure["columns"]:
                mapping_columns.append(f"{col['name']} {col['type']}")
            
            mapping_sql = f"""
                CREATE TABLE IF NOT EXISTS bak.{mapping_table_name} (
                    {', '.join(mapping_columns)}
                );
            """
            cursor.execute(mapping_sql)
            
            # Insert default column mappings
            for mapping in mapping_structure["default_mappings"]:
                placeholders = ", ".join(["%s"] * len(mapping))
                columns = ", ".join(mapping.keys())
                values = list(mapping.values())
                
                cursor.execute(f"""
                    INSERT INTO bak.{mapping_table_name} ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT DO NOTHING;
                """, values)
            
            # Insert sample backup data if available
            data_key = f"{table_type}_backup_data"
            if data_key in backup_fixtures["backup_sample_data"]:
                sample_data = backup_fixtures["backup_sample_data"][data_key]
                
                for record in sample_data:
                    columns = list(record.keys())
                    placeholders = ", ".join(["%s"] * len(columns))
                    column_names = ", ".join(columns)
                    values = list(record.values())
                    
                    cursor.execute(f"""
                        INSERT INTO bak.{backup_table_name} ({column_names})
                        VALUES ({placeholders})
                        ON CONFLICT DO NOTHING;
                    """, values)
            
            return {
                "backup_table": backup_table_name,
                "mapping_table": mapping_table_name,
                "schema": "bak"
            }
            
        finally:
            cursor.close()
            conn.close()
    
    def cleanup_backup_environment(self, table_info: Dict[str, str]) -> None:
        """Clean up backup tables created for testing."""
        conn = self.get_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        
        try:
            schema = table_info["schema"]
            backup_table = table_info["backup_table"]
            mapping_table = table_info["mapping_table"]
            
            cursor.execute(f"DROP TABLE IF EXISTS {schema}.{backup_table};")
            cursor.execute(f"DROP TABLE IF EXISTS {schema}.{mapping_table};")
            
        finally:
            cursor.close()
            conn.close()