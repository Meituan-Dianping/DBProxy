#
# Creating table with multi column index setup
#

--disable_warnings
DROP TABLE IF EXISTS t1;
--enable_warnings

CREATE TABLE t1
(
  key1 INT NOT NULL,
  key2 INT NOT NULL,
  key3 INT NOT NULL,
  key4 INT NOT NULL,
  key5 INT,
  key6 INT,
  key7 INT NOT NULL,
  key8 INT NOT NULL,
  INDEX i1(key1,key2),
  INDEX i2(key2,key1),
  INDEX i3(key3,key4),
  INDEX i4(key4,key3),
  INDEX i5(key5,key6),
  INDEX i6(key6,key5),
  INDEX i7(key7,key8),
  INDEX i8(key8,key7)
);

--disable_query_log


# inserting values

INSERT INTO t1 VALUES ( 1,1,1,1,1,1,1,1),(2,2,2,2,2,2,2,2);

--enable_query_log

# To analyze and store the key distribution of the table.

ANALYZE TABLE t1;

 
