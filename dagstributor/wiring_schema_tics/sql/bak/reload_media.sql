--------------------------------------------------------------------------------
-- restore script
--------------------------------------------------------------------------------

DO $$
DECLARE
    mapping_record RECORD;
    target_column_record RECORD;
    column_count INTEGER;
    rows_inserted INTEGER;
    
    -- Input parameters - SET THESE VALUES
    source_schema TEXT := 'atp';
    source_table TEXT := 'media';
    backup_date TEXT := NULL;        -- NULL = auto-find latest backup
    target_schema TEXT := NULL;      -- NULL = use source_schema
    target_table TEXT := NULL;       -- NULL = use source_table
    
    -- Schema and table configuration
    backup_schema TEXT := 'bak';
    target_schema_final TEXT;
    target_table_final TEXT;
    backup_date_final TEXT;
    
    -- Backup object names
    backup_table_name TEXT;
    column_mapping_table_name TEXT;
    
    -- SQL construction
    select_columns TEXT;
    insert_sql TEXT;
    mapping_query TEXT;
    date_query TEXT;
    
    -- Column mapping storage
    column_mappings RECORD;
BEGIN
    -- Initialize variables
    column_count := 0;
    select_columns := '';
    
    -- Set target schema/table defaults
    target_schema_final := COALESCE(target_schema, source_schema);
    target_table_final := COALESCE(target_table, source_table);
    
    -- Find latest backup date if not provided
    IF backup_date IS NULL THEN
        date_query := 'SELECT SUBSTRING(table_name FROM ''^' || source_schema || '_' || source_table || '_(\d{8})$'') AS backup_date
                       FROM information_schema.tables 
                       WHERE table_schema = ''' || backup_schema || '''
                       AND table_name ~ ''^' || source_schema || '_' || source_table || '_\d{8}$''
                       AND table_type = ''BASE TABLE''
                       ORDER BY backup_date DESC 
                       LIMIT 1';
        
        EXECUTE date_query INTO backup_date_final;
        
        IF backup_date_final IS NULL THEN
            RAISE EXCEPTION 'No backup tables found for %.% in schema %', source_schema, source_table, backup_schema;
        END IF;
        
        RAISE NOTICE 'Auto-selected latest backup date: %', backup_date_final;
    ELSE
        backup_date_final := backup_date;
        RAISE NOTICE 'Using specified backup date: %', backup_date_final;
    END IF;
    
    -- Construct backup object names
    backup_table_name := source_schema || '_' || source_table || '_' || backup_date_final;
    column_mapping_table_name := source_schema || '_' || source_table || '_' || backup_date_final || '_column_mapping';
    
    -- Verify backup table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = backup_schema
        AND table_name = backup_table_name
        AND table_type = 'BASE TABLE'
    ) THEN
        RAISE EXCEPTION 'Backup table %.% does not exist', backup_schema, backup_table_name;
    END IF;
    
    -- Verify column mapping table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = backup_schema
        AND table_name = column_mapping_table_name
        AND table_type = 'BASE TABLE'
    ) THEN
        RAISE EXCEPTION 'Column mapping table %.% does not exist', backup_schema, column_mapping_table_name;
    END IF;
    
    -- Verify target table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = target_schema_final
        AND table_name = target_table_final
        AND table_type = 'BASE TABLE'
    ) THEN
        RAISE EXCEPTION 'Target table %.% does not exist', target_schema_final, target_table_final;
    END IF;
    
    -- Build column mapping in TARGET TABLE ORDER
    FOR target_column_record IN
        SELECT column_name, ordinal_position
        FROM information_schema.columns
        WHERE table_schema = target_schema_final
        AND table_name = target_table_final
        ORDER BY ordinal_position
    LOOP
        -- Look up this column in our mapping table
        mapping_query := 'SELECT source_column_name, bak_column_name, source_pgsql_data_type, bak_pgsql_data_type, enum_name 
                          FROM ' || backup_schema || '.' || column_mapping_table_name || ' 
                          WHERE source_column_name = ''' || target_column_record.column_name || '''';
        
        EXECUTE mapping_query INTO mapping_record;
        
        -- Skip if this column is not in the mapping (source_column_name is NULL)
        IF mapping_record.source_column_name IS NULL THEN
            CONTINUE;
        END IF;
        
        -- Build SELECT clause
        IF column_count > 0 THEN
            select_columns := select_columns || ', ';
        END IF;
        
        -- Handle enum conversion based on column mapping info
        IF mapping_record.enum_name IS NOT NULL AND mapping_record.bak_pgsql_data_type = 'text' THEN
            -- This was an enum converted to text during backup, cast back to enum
            select_columns := select_columns || mapping_record.bak_column_name || '::text::' || 
                target_schema_final || '.' || mapping_record.enum_name;
        ELSE
            -- Regular column, no conversion needed
            select_columns := select_columns || mapping_record.bak_column_name;
        END IF;
        
        column_count := column_count + 1;
    END LOOP;
    
    -- Verify we have columns to restore
    IF column_count = 0 THEN
        RAISE EXCEPTION 'No valid columns found in column mapping table for restore';
    END IF;
    
    -- Construct and execute INSERT statement
    insert_sql := 'INSERT INTO ' || target_schema_final || '.' || target_table_final || 
                  ' SELECT ' || select_columns || 
                  ' FROM ' || backup_schema || '.' || backup_table_name;
    
    RAISE NOTICE 'Executing restore: %', insert_sql;
    EXECUTE insert_sql;
    
    -- Get the number of rows inserted
    GET DIAGNOSTICS rows_inserted = ROW_COUNT;
    
    RAISE NOTICE 'Successfully restored % rows from %.% to %.%', 
        rows_inserted, backup_schema, backup_table_name, target_schema_final, target_table_final;
        
END
$$;

--------------------------------------------------------------------------------
-- end of restore script
--------------------------------------------------------------------------------