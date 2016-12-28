
SET autocommit=0;
SELECT * FROM t1 FOR UPDATE;
-- if ($con1_extra_sql_present) {
	-- eval $con1_extra_sql
-- }

SET autocommit=0;
SELECT * FROM t2 FOR UPDATE;
-- if ($con2_extra_sql_present) {
	-- eval $con2_extra_sql
-- }

-- if ($con1_should_be_rolledback) {
	-- send
	INSERT INTO t2 VALUES (0);

	INSERT INTO t1 VALUES (0);
	ROLLBACK;

	-- error ER_LOCK_DEADLOCK
	-- reap
-- }
# else
-- if (!$con1_should_be_rolledback) {
	-- send
	INSERT INTO t1 VALUES (0);

	INSERT INTO t2 VALUES (0);
	ROLLBACK;

	-- error ER_LOCK_DEADLOCK
	-- reap
-- }



