# Setup connections to both MySQL Servers connected to the cluster



# Check that server1 has NDB  support
let $engines_table= query_get_value(SHOW TABLES FROM information_schema LIKE 'ENGINES', Tables_in_information_schema (ENGINES), 1);
disable_query_log;
if (`SELECT 1 FROM dual WHERE '$engines_table' = 'engines'`)
{
--require r/true.require
SELECT (support = 'YES' or support = 'DEFAULT' or support = 'ENABLED') as `TRUE` FROM information_schema.engines WHERE engine = 'ndbcluster';
--source include/ndb_not_readonly.inc
}
enable_query_log;

# Check that server2 has NDB support
let $engines_table= query_get_value(SHOW TABLES FROM information_schema LIKE 'ENGINES', Tables_in_information_schema (ENGINES), 1);
disable_query_log;
if (`SELECT 1 FROM dual WHERE '$engines_table' = 'engines'`)
{
--require r/true.require
SELECT (support = 'YES' or support = 'DEFAULT' or support = 'ENABLED') as `TRUE` FROM information_schema.engines WHERE engine = 'ndbcluster';
--source include/ndb_not_readonly.inc
}
enable_query_log;

# cleanup

disable_query_log;
disable_warnings;
--error 0,1051
drop table if exists t1,t2,t3,t4,t5,t6,t7,t8,t9,t10;
flush tables;
flush status;
enable_warnings;
enable_query_log;

disable_query_log;
disable_warnings;
--error 0,1051
drop table if exists t1,t2,t3,t4,t5,t6,t7,t8,t9,t10;
flush tables;
flush status;
enable_warnings;
enable_query_log;

# Set the default connection
