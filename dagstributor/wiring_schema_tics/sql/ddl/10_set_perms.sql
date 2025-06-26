--------------------------------------------------------------------------------
-- table creation statement
--
-- sets permissions for users within the atp schema
-- DOES NOT create any users or set any passwords
-- does not set any permissions for any other schem
--------------------------------------------------------------------------------

GRANT USAGE ON SCHEMA atp TO "rear_diff";
GRANT SELECT ON atp.training TO "rear_diff";
GRANT UPDATE (label, updated_at) ON atp.training TO "rear_diff";

--------------------------------------------------------------------------------
-- end of 10_set_perms.sql
--------------------------------------------------------------------------------