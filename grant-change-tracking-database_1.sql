-- Run in database_1 as admin. Grants "test" permission to query change tracking.
-- Required for CHANGETABLE() and change tracking system views.

GRANT VIEW CHANGE TRACKING ON SCHEMA::dbo TO test;
