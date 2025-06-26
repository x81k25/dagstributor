--------------------------------------------------------------------------------
-- restore script
--------------------------------------------------------------------------------

DO $$
DECLARE
    mapping_record RECORD;
    column_count INTEGER;
    
    -- Input parameters - SET THESE VALUES
    source_schema TEXT := 'atp';
    source_table TEXT := 'prediction';
    backup_date TEXT := '20241226';  -- Format: YYYYMMDD
    target_schema TEXT := NULL;      -- NULL = use source_schema
    target_table TEXT := NULL;       -- NULL = use source_table
    
    -- Schema and table configuration
    backup_schema TEXT := 'bak';
    target_schema_final TEXT;
    target_table_final TEXT;
    
    -- Backup object names
    backup_table_name TEXT;
    column_mapping_table_name TEXT;
    
    -- SQL construction
    select_columns TEXT;
    insert_sql TEXT;
BEGIN
    -- Initialize variables
    column_count := 0;
    select_columns := '';
    
    -- Set target schema/table defaults
    target_schema_final := COALESCE(target_schema, source_schema);
    target_table_final := COALESCE(target_table, source_table);
    
    -- Construct backup object names
    backup_table_name := source_schema || '_' || source_table || '_' || backup_date;
    column_mapping_table_name := source_schema || '_' || source_table || '_' || backup_date || '_column_mapping';
    
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
    
    -- Build column mapping from backup to target
    FOR mapping_record IN
        SELECT 
            source_column_name,
            bak_column_name,
            source_pgsql_data_type,
            bak_pgsql_data_type,
            enum_name
        FROM bak.|| column_mapping_table_name
        WHERE source_column_name IS NOT NULL
        ORDER BY source_column_name
    LOOP
        -- Verify target column exists
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = target_schema_final
            AND table_name = target_table_final
            AND column_name = mapping_record.source_column_name
        ) THEN
            RAISE EXCEPTION 'Target column %.%.% does not exist', 
                target_schema_final, target_table_final, mapping_record.source_column_name;
        END IF;
        
        -- Build SELECT clause
        IF column_count > 0 THEN
            select_columns := select_columns || ', ';
        END IF;
        
        -- Handle enum conversion back from text
        IF mapping_record.enum_name IS NOT NULL THEN
            select_columns := select_columns || mapping_record.bak_column_name || '::' || 
                target_schema_final || '.' || mapping_record.enum_name || ' AS ' || mapping_record.source_column_name;
        ELSE
            select_columns := select_columns || mapping_record.bak_column_name || ' AS ' || mapping_record.source_column_name;
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
    
    RAISE NOTICE 'Successfully restored % rows from %.% to %.%', 
        ROW_COUNT, backup_schema, backup_table_name, target_schema_final, target_table_final;
        
END
$$;

--------------------------------------------------------------------------------
-- end of restore script
--------------------------------------------------------------------------------