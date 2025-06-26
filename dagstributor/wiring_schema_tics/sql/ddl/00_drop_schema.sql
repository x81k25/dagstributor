--------------------------------------------------------------------------------
-- if schema and objects are currently on an older version use this script to
--   clean all existing elements before running the subsequent scripts
--
-- THIS WILL DELETE all objects and data for this project
--
-- see bak.sql for commands to back-up data before dropping and rebuilding and
--   rebuilding the schema; not, not all schema updates are backwards c
--   compatible, therefore using the translations from the backup objects and
--   new objects may not be a 1:1 select and drop statement
--------------------------------------------------------------------------------

DROP SCHEMA IF EXISTS atp CASCADE;

--------------------------------------------------------------------------------
-- end of _00_drop_schema.sql
--------------------------------------------------------------------------------