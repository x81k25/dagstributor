--------------------------------------------------------------------------------
-- schema config
--------------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS bak;

--------------------------------------------------------------------------------
-- backup operation with metadata tables
--------------------------------------------------------------------------------

DO $$
DECLARE
    table_record RECORD;
    column_record RECORD;
    current_date_str TEXT;
    source_schema TEXT := 'atp';
    target_schema TEXT := 'bak';
    table_to_backup TEXT := 'training'; -- Specify the table name here
    new_table_name TEXT;
    metadata_table_name TEXT;
    column_mapping_table_name TEXT;
    select_clause TEXT;
    enum_columns TEXT[];
BEGIN
    -- Get current date in YYYYMMDD format
    current_date_str := TO_CHAR(CURRENT_DATE, 'YYYYMMDD');

    -- Create the target schema if it doesn't exist
    EXECUTE 'CREATE SCHEMA IF NOT EXISTS ' || target_schema;

    -- Check if the specified table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = source_schema
        AND table_name = table_to_backup
        AND table_type = 'BASE TABLE'
    ) THEN
        RAISE EXCEPTION 'Table %.% does not exist', source_schema, table_to_backup;
    END IF;

	-- Create table names
    new_table_name := source_schema || '_' || table_to_backup || '_' || current_date_str;
    metadata_table_name := source_schema || '_' || table_to_backup || '_' || current_date_str || '_metadata' ;
    column_mapping_table_name := source_schema || '_' || table_to_backup  || '_' || current_date_str || '_column_mapping';

    -- Drop existing tables if they exist (overwrite same day backups)
    EXECUTE 'DROP TABLE IF EXISTS ' || target_schema || '.' || new_table_name;
    EXECUTE 'DROP TABLE IF EXISTS ' || target_schema || '.' || metadata_table_name;
    EXECUTE 'DROP TABLE IF EXISTS ' || target_schema || '.' || column_mapping_table_name;

    -- Create metadata table
    EXECUTE 'CREATE TABLE ' || target_schema || '.' || metadata_table_name || ' (
        backup_date DATE,
        source_schema TEXT,
        source_table TEXT,
        backup_table TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )';

    -- Insert metadata
    EXECUTE 'INSERT INTO ' || target_schema || '.' || metadata_table_name || 
            ' (backup_date, source_schema, source_table, backup_table) VALUES (''' ||
            CURRENT_DATE || ''', ''' || source_schema || ''', ''' || table_to_backup || ''', ''' || new_table_name || ''')';

    -- Create column mapping table
    EXECUTE 'CREATE TABLE ' || target_schema || '.' || column_mapping_table_name || ' (
        source_column_name TEXT,
        bak_column_name TEXT,
        source_pgsql_data_type TEXT,
        bak_pgsql_data_type TEXT,
        enum_name TEXT
    )';

    -- Reset for the table
    select_clause := '';
    enum_columns := ARRAY[]::TEXT[];

    -- Build SELECT clause and populate column mapping
    FOR column_record IN
        SELECT
            c.column_name,
            c.data_type,
            c.udt_name
        FROM information_schema.columns c
        WHERE c.table_schema = source_schema
        AND c.table_name = table_to_backup
        ORDER BY c.ordinal_position
    LOOP
        -- Check if column is an enum type
        IF column_record.data_type = 'USER-DEFINED' AND
           EXISTS (
               SELECT 1 FROM pg_type t
               JOIN pg_namespace n ON t.typnamespace = n.oid
               WHERE t.typname = column_record.udt_name
               AND n.nspname = source_schema
               AND t.typtype = 'e'
           ) THEN
            -- Convert enum to text
            IF select_clause != '' THEN
                select_clause := select_clause || ', ';
            END IF;
            select_clause := select_clause || column_record.column_name || '::text AS ' || column_record.column_name;
            enum_columns := array_append(enum_columns, column_record.column_name);
            
            -- Insert enum column mapping
            EXECUTE 'INSERT INTO ' || target_schema || '.' || column_mapping_table_name || 
                    ' (source_column_name, bak_column_name, source_pgsql_data_type, bak_pgsql_data_type, enum_name) VALUES (''' ||
                    column_record.column_name || ''', ''' || column_record.column_name || ''', ''' ||
                    column_record.udt_name || ''', ''text'', ''' || column_record.udt_name || ''')';
        ELSE
            -- Regular column
            IF select_clause != '' THEN
                select_clause := select_clause || ', ';
            END IF;
            select_clause := select_clause || column_record.column_name;
            
            -- Insert regular column mapping
            EXECUTE 'INSERT INTO ' || target_schema || '.' || column_mapping_table_name || 
                    ' (source_column_name, bak_column_name, source_pgsql_data_type, bak_pgsql_data_type, enum_name) VALUES (''' ||
                    column_record.column_name || ''', ''' || column_record.column_name || ''', ''' ||
                    column_record.data_type || ''', ''' || column_record.data_type || ''', NULL)';
        END IF;
    END LOOP;

    -- Create a copy of the table in the target schema with enum conversion
    EXECUTE 'CREATE TABLE ' || target_schema || '.' || new_table_name || ' AS
             SELECT ' || select_clause || ' FROM ' || source_schema || '.' || table_to_backup;

    -- Log the backup creation
    IF array_length(enum_columns, 1) > 0 THEN
        RAISE NOTICE 'Created backup: %.% from %.% (converted enums: %)',
                     target_schema, new_table_name,
                     source_schema, table_to_backup,
                     array_to_string(enum_columns, ', ');
    ELSE
        RAISE NOTICE 'Created backup: %.% from %.%',
                     target_schema, new_table_name,
                     source_schema, table_to_backup;
    END IF;

    RAISE NOTICE 'Created metadata table: %.%', target_schema, metadata_table_name;
    RAISE NOTICE 'Created column mapping table: %.%', target_schema, column_mapping_table_name;
    RAISE NOTICE 'Backup complete for table "%"', table_to_backup;
END
$$;

--------------------------------------------------------------------------------
-- end of backup script
--------------------------------------------------------------------------------