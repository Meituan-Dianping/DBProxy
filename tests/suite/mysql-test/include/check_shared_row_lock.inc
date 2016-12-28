#
# SUMMARY
#   Check if statement reading table '$table' takes shared locks
#   on some of its rows.
#
# PARAMETERS 
#   $table          Table for which presence of row locks should be checked.
#   $con_aux        Name of auxiliary connection to be used by this script.
#   $statement      Statement to be checked.
#   $wait_statement Sub-statement which is supposed to acquire locks (should
#                   be the same as $statement for ordinary statements).
#
# EXAMPLE
#    innodb_mysql_lock2.test
#
--disable_result_log
--disable_query_log

begin;
--eval select * from $table for update;

begin;
--send_eval $statement;

--enable_result_log
--enable_query_log

# Wait until statement is successfully blocked because
# all rows in table are X-locked. This means that at
# least it acquires S-locks on some of rows.
let $wait_condition=
  select count(*) = 1 from information_schema.processlist
  where state in ("Sending data","statistics", "preparing") and
        info = "$wait_statement";

--disable_result_log
--disable_query_log

rollback;

--reap
rollback;

--enable_result_log
--enable_query_log

if ($success)
{
--echo Success: '$statement' takes shared row locks on '$table'.
}

if (!$success)
{
--echo Error: '$statement' hasn't taken shared row locks on '$table'!
}
