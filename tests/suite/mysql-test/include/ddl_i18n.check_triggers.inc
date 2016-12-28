#   - Check SHOW CREATE statement;

--echo
--echo

SHOW CREATE TRIGGER trg1|
--echo
SHOW CREATE TRIGGER trg2|
--echo
SHOW CREATE TRIGGER mysqltest2.trg3|
--echo
SHOW CREATE TRIGGER mysqltest2.trg4|

#   - Check SHOW statement;

--echo
--echo

SHOW TRIGGERS|

--echo


--echo

SHOW TRIGGERS|


#   - Check INFORMATION_SCHEMA;

--echo
--echo

--replace_column 17 CREATED
SELECT * FROM INFORMATION_SCHEMA.TRIGGERS WHERE trigger_name = 'trg1'|

--echo
--replace_column 17 CREATED
SELECT * FROM INFORMATION_SCHEMA.TRIGGERS WHERE trigger_name = 'trg2'|

--echo
--replace_column 17 CREATED
SELECT * FROM INFORMATION_SCHEMA.TRIGGERS WHERE trigger_name = 'trg3'|

--echo
--replace_column 17 CREATED
SELECT * FROM INFORMATION_SCHEMA.TRIGGERS WHERE trigger_name = 'trg4'|

#   - Initialize the used variables (actual values don't matter);

--echo
--echo



#   - Execute the triggers;

--echo
--echo

INSERT INTO t1 VALUES(1)|

--echo
--echo ---> Log:
SELECT msg FROM log|

--echo

INSERT INTO mysqltest2.t1 VALUES(1)|

--echo
--echo ---> Log:
SELECT msg FROM mysqltest2.log|

