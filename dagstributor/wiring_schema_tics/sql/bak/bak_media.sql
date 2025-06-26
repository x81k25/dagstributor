--------------------------------------------------------------------------------
-- schema config
--------------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS bak;

--------------------------------------------------------------------------------
-- copy operation config
--------------------------------------------------------------------------------

DO $$
DECLARE
    table_record RECORD;
    column_record RECORD;
    current_date_str TEXT;
    source_schema TEXT := 'atp';
    target_schema TEXT := 'bak';
    table_to_backup TEXT := 'media'; -- specify table name here
    new_table_name TEXT;
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

    -- Reset for the table
    select_clause := '';
    enum_columns := ARRAY[]::TEXT[];

    -- Build SELECT clause with enum conversion for the specified table
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
        -- [Rest of the column processing logic remains the same]
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
        ELSE
            -- Regular column
            IF select_clause != '' THEN
                select_clause := select_clause || ', ';
            END IF;
            select_clause := select_clause || column_record.column_name;
        END IF;
    END LOOP;

    -- Create the new table name with format
    new_table_name := source_schema || '_' || table_to_backup || '_' || current_date_str;

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

    RAISE NOTICE 'Backup complete for table "%"', table_to_backup;
END
$$;

--------------------------------------------------------------------------------
-- end of bak.sql
--------------------------------------------------------------------------------