# MRR tests that are special for InnoDB

--echo #
--echo # Bug#41029 "MRR: SELECT FOR UPDATE fails to lock gaps (InnoDB table)"
--echo #

# This test verifies that a SELECT FOR UPDATE statement executed in
# REPEATABLE READ isolation will lock the entire read interval by verifying
# that a second transaction trying to update data within this interval will
# be blocked.



SET AUTOCOMMIT=0;

CREATE TABLE t1 (
  dummy INT PRIMARY KEY,
  a INT UNIQUE,
  b INT
) ENGINE=InnoDB;

INSERT INTO t1 VALUES (1,1,1),(3,3,3),(5,5,5);
COMMIT;

SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT @@tx_isolation;
START TRANSACTION;

EXPLAIN SELECT * FROM t1 WHERE a > 2 FOR UPDATE;

SELECT * FROM t1 WHERE a > 2 FOR UPDATE;


SET AUTOCOMMIT=0;
START TRANSACTION;

--error ER_LOCK_WAIT_TIMEOUT
INSERT INTO t1 VALUES (2,2,2);
ROLLBACK;


ROLLBACK;
DROP TABLE t1;


--echo #
--echo # Bug#54286 "Server crash at lock timeout with MRR"
--echo #



SET AUTOCOMMIT=0;

CREATE TABLE t1 (
  dummy INT PRIMARY KEY,
  a INT UNIQUE,
  b INT
) ENGINE=InnoDB;

COMMIT;

INSERT INTO t1 VALUES (1,1,1),(3,3,3),(5,5,5);


SET AUTOCOMMIT=0;

--error ER_LOCK_WAIT_TIMEOUT
SELECT * FROM t1 WHERE a > 2 FOR UPDATE;
ROLLBACK;


ROLLBACK;
DROP TABLE t1;
COMMIT;


