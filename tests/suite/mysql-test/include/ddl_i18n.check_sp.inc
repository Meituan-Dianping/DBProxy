#   - Check SHOW CREATE statement;

--echo
--echo

SHOW CREATE PROCEDURE p1|
--echo
SHOW CREATE PROCEDURE p2|
--echo
SHOW CREATE PROCEDURE mysqltest2.p3|
--echo
SHOW CREATE PROCEDURE mysqltest2.p4|

#   - Check SHOW statement;

--echo
--echo

--replace_column 5 MODIFIED 6 CREATED
SHOW PROCEDURE STATUS LIKE 'p1'|

--echo
--replace_column 5 MODIFIED 6 CREATED
SHOW PROCEDURE STATUS LIKE 'p2'|

--echo
--replace_column 5 MODIFIED 6 CREATED
SHOW PROCEDURE STATUS LIKE 'p3'|

--echo
--replace_column 5 MODIFIED 6 CREATED
SHOW PROCEDURE STATUS LIKE 'p4'|

#   - Check INFORMATION_SCHEMA;

--echo
--echo

--replace_column 24 CREATED 25 ALTERED
SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE routine_name = 'p1'|

--echo
--replace_column 24 CREATED 25 ALTERED
SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE routine_name = 'p2'|

--echo
--replace_column 24 CREATED 25 ALTERED
SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE routine_name = 'p3'|

--echo
--replace_column 24 CREATED 25 ALTERED
SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE routine_name = 'p4'|



