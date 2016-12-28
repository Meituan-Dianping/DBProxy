
###########################################################################

--echo
--echo TINYINT
--echo

CREATE PROCEDURE p1(OUT v TINYINT)
  SET v = 127;







DROP PROCEDURE p1;

###########################################################################

--echo
--echo SMALLINT
--echo

CREATE PROCEDURE p1(OUT v SMALLINT)
  SET v = 32767;







DROP PROCEDURE p1;

###########################################################################

--echo
--echo MEDIUMINT
--echo

CREATE PROCEDURE p1(OUT v MEDIUMINT)
  SET v = 8388607;







DROP PROCEDURE p1;

###########################################################################

--echo
--echo INT
--echo

CREATE PROCEDURE p1(OUT v INT)
  SET v = 2147483647;







DROP PROCEDURE p1;

###########################################################################

--echo
--echo BIGINT
--echo

CREATE PROCEDURE p1(OUT v BIGINT)
  SET v = 9223372036854775807;







DROP PROCEDURE p1;

###########################################################################

--echo
--echo BIT(11)
--echo

CREATE PROCEDURE p1(OUT v BIT(11))
  SET v = b'10100100101';







DROP PROCEDURE p1;

###########################################################################

--echo
--echo TIMESTAMP
--echo

CREATE PROCEDURE p1(OUT v TIMESTAMP)
  SET v = '2007-11-18 15:01:02';







DROP PROCEDURE p1;

###########################################################################

--echo
--echo DATETIME
--echo

CREATE PROCEDURE p1(OUT v DATETIME)
  SET v = '1234-11-12 12:34:59';







DROP PROCEDURE p1;

###########################################################################

--echo
--echo TIME
--echo

CREATE PROCEDURE p1(OUT v TIME)
  SET v = '123:45:01';







DROP PROCEDURE p1;

###########################################################################

--echo
--echo DATE
--echo

CREATE PROCEDURE p1(OUT v DATE)
  SET v = '1234-11-12';







DROP PROCEDURE p1;

###########################################################################

--echo
--echo YEAR
--echo

CREATE PROCEDURE p1(OUT v YEAR)
  SET v = 2010;







DROP PROCEDURE p1;

###########################################################################

--echo
--echo FLOAT(7, 4)
--echo

CREATE PROCEDURE p1(OUT v FLOAT(7, 4))
  SET v = 123.4567;


DROP PROCEDURE p1;

###########################################################################

--echo
--echo DOUBLE(8, 5)
--echo

CREATE PROCEDURE p1(OUT v DOUBLE(8, 5))
  SET v = 123.45678;







DROP PROCEDURE p1;

###########################################################################

--echo
--echo DECIMAL(9, 6)
--echo

CREATE PROCEDURE p1(OUT v DECIMAL(9, 6))
  SET v = 123.456789;







DROP PROCEDURE p1;

###########################################################################

--echo
--echo CHAR(32)
--echo

CREATE PROCEDURE p1(OUT v CHAR(32))
  SET v = REPEAT('a', 16);







DROP PROCEDURE p1;

###########################################################################

--echo
--echo VARCHAR(32)
--echo

CREATE PROCEDURE p1(OUT v VARCHAR(32))
  SET v = REPEAT('b', 16);







DROP PROCEDURE p1;

###########################################################################

--echo
--echo TINYTEXT
--echo

CREATE PROCEDURE p1(OUT v TINYTEXT)
  SET v = REPEAT('c', 16);







DROP PROCEDURE p1;

###########################################################################

--echo
--echo TEXT
--echo

CREATE PROCEDURE p1(OUT v TEXT)
  SET v = REPEAT('d', 16);







DROP PROCEDURE p1;

###########################################################################

--echo
--echo MEDIUMTEXT
--echo

CREATE PROCEDURE p1(OUT v MEDIUMTEXT)
  SET v = REPEAT('e', 16);







DROP PROCEDURE p1;

###########################################################################

--echo
--echo LONGTEXT
--echo

CREATE PROCEDURE p1(OUT v LONGTEXT)
  SET v = REPEAT('f', 16);







DROP PROCEDURE p1;

###########################################################################

--echo
--echo BINARY(32)
--echo

CREATE PROCEDURE p1(OUT v BINARY(32))
  SET v = REPEAT('g', 32);







DROP PROCEDURE p1;

###########################################################################

--echo
--echo VARBINARY(32)
--echo

CREATE PROCEDURE p1(OUT v VARBINARY(32))
  SET v = REPEAT('h', 16);







DROP PROCEDURE p1;

###########################################################################

--echo
--echo TINYBLOB
--echo

CREATE PROCEDURE p1(OUT v TINYBLOB)
  SET v = REPEAT('i', 16);







DROP PROCEDURE p1;

###########################################################################

--echo
--echo BLOB
--echo

CREATE PROCEDURE p1(OUT v BLOB)
  SET v = REPEAT('j', 16);







DROP PROCEDURE p1;

###########################################################################

--echo
--echo MEDIUMBLOB
--echo

CREATE PROCEDURE p1(OUT v MEDIUMBLOB)
  SET v = REPEAT('k', 16);







DROP PROCEDURE p1;

###########################################################################

--echo
--echo LONGBLOB
--echo

CREATE PROCEDURE p1(OUT v LONGBLOB)
  SET v = REPEAT('l', 16);







DROP PROCEDURE p1;

###########################################################################

--echo
--echo SET('aaa', 'bbb')
--echo

CREATE PROCEDURE p1(OUT v SET('aaa', 'bbb'))
  SET v = 'aaa';







DROP PROCEDURE p1;

###########################################################################

--echo
--echo ENUM('aaa', 'bbb')
--echo

CREATE PROCEDURE p1(OUT v ENUM('aaa', 'bbb'))
  SET v = 'aaa';







DROP PROCEDURE p1;
