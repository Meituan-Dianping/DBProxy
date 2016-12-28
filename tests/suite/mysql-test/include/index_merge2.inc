# include/index_merge2.inc
#
# Index merge tests
#
# The variable
#     $engine_type       -- storage engine to be tested
# has to be set before sourcing this script.
#
# Note: The comments/expectations refer to InnoDB.
#       They might be not valid for other storage engines.
#
# Last update:
# 2006-08-02 ML test refactored
#               old name was t/index_merge_innodb.test
#               main code went into include/index_merge2.inc
#

--echo #---------------- Index merge test 2 -------------------------------------------

eval SET SESSION DEFAULT_STORAGE_ENGINE = $engine_type;

--disable_warnings
drop table if exists t1,t2;
--enable_warnings

create table t1
(
  key1 int not null,
  key2 int not null,

  INDEX i1(key1),
  INDEX i2(key2)
);

--disable_query_log
let $1=200;
while ($1)
{
  eval insert into t1 values (200-$1, $1);
  dec $1;
}
--enable_query_log

-- disable_query_log
-- disable_result_log
analyze table t1;
-- enable_result_log
-- enable_query_log

# No primary key
explain select * from t1 where key1 < 5 or key2 > 197;

select * from t1 where key1 < 5 or key2 > 197;

explain select * from t1 where key1 < 3 or key2 > 195;
select * from t1 where key1 < 3 or key2 > 195;

# Primary key as case-sensitive string with \0s.
# also make primary key be longer then max. index length of MyISAM.
alter table t1 add str1 char (255) not null,
                add zeroval int not null default 0,
                add str2 char (255) not null,
                add str3 char (255) not null;

update t1 set str1='aaa', str2='bbb', str3=concat(key2, '-', key1 div 2, '_' ,if(key1 mod 2 = 0, 'a', 'A')) where key1 is not null;

alter table t1 add primary key (str1, zeroval, str2, str3);

-- disable_query_log
-- disable_result_log
analyze table t1;
-- enable_result_log
-- enable_query_log

explain select * from t1 where key1 < 5 or key2 > 197;

select * from t1 where key1 < 5 or key2 > 197;

explain select * from t1 where key1 < 3 or key2 > 195;
select * from t1 where key1 < 3 or key2 > 195;

# Test for BUG#5401
drop table t1;
create table t1 (
  pk    integer not null auto_increment primary key,
  key1  integer,
  key2  integer not null,
  filler char  (200),
  index (key1),
  index (key2)
);
--disable_query_log
let $1=30;
while ($1)
{
  eval insert into t1 (key1, key2, filler) values ($1/4, $1/8, 'filler-data');
  dec $1;
}
--enable_query_log

-- disable_query_log
-- disable_result_log
analyze table t1;
-- enable_result_log
-- enable_query_log

explain select pk from t1 where key1 = 1 and key2 = 1;
select pk from t1 where key2 = 1 and key1 = 1;
select pk from t1 ignore index(key1,key2) where key2 = 1 and key1 = 1;

# More tests for BUG#5401.
drop table t1;
create table t1 (
  pk int primary key auto_increment,
  key1a  int,
  key2a  int,
  key1b  int,
  key2b  int,
  dummy1 int,
  dummy2 int,
  dummy3 int,
  dummy4 int,
  key3a  int,
  key3b  int,
  filler1 char (200),
  index i1(key1a, key1b),
  index i2(key2a, key2b),
  index i3(key3a, key3b)
);

create table t2 (a int);
insert into t2 values (0),(1),(2),(3),(4),(NULL);

insert into t1 (key1a, key1b, key2a, key2b, key3a, key3b)
  select A.a, B.a, C.a, D.a, C.a, D.a from t2 A,t2 B,t2 C, t2 D;
insert into t1 (key1a, key1b, key2a, key2b, key3a, key3b)
  select key1a, key1b, key2a, key2b, key3a, key3b from t1;
insert into t1 (key1a, key1b, key2a, key2b, key3a, key3b)
  select key1a, key1b, key2a, key2b, key3a, key3b from t1;
analyze table t1;
select count(*) from t1;

-- disable_query_log
-- disable_result_log
analyze table t2;
-- enable_result_log
-- enable_query_log

if ($index_merge_random_rows_in_EXPLAIN)
{
  --replace_column 9 #
}
explain select count(*) from t1 where
  key1a = 2 and key1b is null and  key2a = 2 and key2b is null;

select count(*) from t1 where
  key1a = 2 and key1b is null and key2a = 2 and key2b is null;

if ($index_merge_random_rows_in_EXPLAIN)
{
  --replace_column 9 #
}
explain select count(*) from t1 where
  key1a = 2 and key1b is null and key3a = 2 and key3b is null;

select count(*) from t1 where
  key1a = 2 and key1b is null and key3a = 2 and key3b is null;

drop table t1,t2;

# Test for BUG#8441
create table t1 (
  id1 int,
  id2 date ,
  index idx2 (id1,id2),
  index idx1 (id2)
);
insert into t1 values(1,'20040101'), (2,'20040102');
select * from t1  where id1 = 1  and id2= '20040101';
drop table t1;

# Test for BUG#12720
--disable_warnings
drop view if exists v1;
--enable_warnings
CREATE TABLE t1 (
  `oid` int(11) unsigned NOT NULL auto_increment,
  `fk_bbk_niederlassung` int(11) unsigned NOT NULL,
  `fk_wochentag` int(11) unsigned NOT NULL,
  `uhrzeit_von` time NOT NULL COMMENT 'HH:MM',
  `uhrzeit_bis` time NOT NULL COMMENT 'HH:MM',
  `geloescht` tinyint(4) NOT NULL,
  `version` int(5) NOT NULL,
  PRIMARY KEY  (`oid`),
  KEY `fk_bbk_niederlassung` (`fk_bbk_niederlassung`),
  KEY `fk_wochentag` (`fk_wochentag`),
  KEY `ix_version` (`version`)
) DEFAULT CHARSET=latin1;

insert  into t1 values
(1, 38, 1, '08:00:00', '13:00:00', 0, 1),
(2, 38, 2, '08:00:00', '13:00:00', 0, 1),
(3, 38, 3, '08:00:00', '13:00:00', 0, 1),
(4, 38, 4, '08:00:00', '13:00:00', 0, 1),
(5, 38, 5, '08:00:00', '13:00:00', 0, 1),
(6, 38, 5, '08:00:00', '13:00:00', 1, 2),
(7, 38, 3, '08:00:00', '13:00:00', 1, 2),
(8, 38, 1, '08:00:00', '13:00:00', 1, 2),
(9, 38, 2, '08:00:00', '13:00:00', 1, 2),
(10, 38, 4, '08:00:00', '13:00:00', 1, 2),
(11, 38, 1, '08:00:00', '13:00:00', 0, 3),
(12, 38, 2, '08:00:00', '13:00:00', 0, 3),
(13, 38, 3, '08:00:00', '13:00:00', 0, 3),
(14, 38, 4, '08:00:00', '13:00:00', 0, 3),
(15, 38, 5, '08:00:00', '13:00:00', 0, 3),
(16, 38, 4, '08:00:00', '13:00:00', 0, 4),
(17, 38, 5, '08:00:00', '13:00:00', 0, 4),
(18, 38, 1, '08:00:00', '13:00:00', 0, 4),
(19, 38, 2, '08:00:00', '13:00:00', 0, 4),
(20, 38, 3, '08:00:00', '13:00:00', 0, 4),
(21, 7, 1, '08:00:00', '13:00:00', 0, 1),
(22, 7, 2, '08:00:00', '13:00:00', 0, 1),
(23, 7, 3, '08:00:00', '13:00:00', 0, 1),
(24, 7, 4, '08:00:00', '13:00:00', 0, 1),
(25, 7, 5, '08:00:00', '13:00:00', 0, 1);

create view v1 as
select
  zeit1.oid AS oid,
  zeit1.fk_bbk_niederlassung AS fk_bbk_niederlassung,
  zeit1.fk_wochentag AS fk_wochentag,
  zeit1.uhrzeit_von AS uhrzeit_von,
  zeit1.uhrzeit_bis AS uhrzeit_bis,
  zeit1.geloescht AS geloescht,
  zeit1.version AS version
from
  t1 zeit1
where
(zeit1.version =
  (select max(zeit2.version) AS `max(version)`
   from t1 zeit2
   where
     ((zeit1.fk_bbk_niederlassung = zeit2.fk_bbk_niederlassung) and
      (zeit1.fk_wochentag = zeit2.fk_wochentag) and
      (zeit1.uhrzeit_von = zeit2.uhrzeit_von) and
      (zeit1.uhrzeit_bis = zeit2.uhrzeit_bis)
     )
  )
)
and (zeit1.geloescht = 0);

select * from v1 where oid = 21;
drop view v1;
drop table t1;
##
CREATE TABLE t1(
  t_cpac varchar(2) NOT NULL,
  t_vers varchar(4) NOT NULL,
  t_rele varchar(2) NOT NULL,
  t_cust varchar(4) NOT NULL,
  filler1 char(250) default NULL,
  filler2 char(250) default NULL,
  PRIMARY KEY (t_cpac,t_vers,t_rele,t_cust),
  UNIQUE KEY IX_4 (t_cust,t_cpac,t_vers,t_rele),
  KEY IX_5 (t_vers,t_rele,t_cust)
);

insert into t1 values
('tm','2.5 ','a ','    ','',''), ('tm','2.5U','a ','stnd','',''),
('da','3.3 ','b ','    ','',''), ('da','3.3U','b ','stnd','',''),
('tl','7.6 ','a ','    ','',''), ('tt','7.6 ','a ','    ','',''),
('bc','B61 ','a ','    ','',''), ('bp','B61 ','a ','    ','',''),
('ca','B61 ','a ','    ','',''), ('ci','B61 ','a ','    ','',''),
('cp','B61 ','a ','    ','',''), ('dm','B61 ','a ','    ','',''),
('ec','B61 ','a ','    ','',''), ('ed','B61 ','a ','    ','',''),
('fm','B61 ','a ','    ','',''), ('nt','B61 ','a ','    ','',''),
('qm','B61 ','a ','    ','',''), ('tc','B61 ','a ','    ','',''),
('td','B61 ','a ','    ','',''), ('tf','B61 ','a ','    ','',''),
('tg','B61 ','a ','    ','',''), ('ti','B61 ','a ','    ','',''),
('tp','B61 ','a ','    ','',''), ('ts','B61 ','a ','    ','',''),
('wh','B61 ','a ','    ','',''), ('bc','B61U','a ','stnd','',''),
('bp','B61U','a ','stnd','',''), ('ca','B61U','a ','stnd','',''),
('ci','B61U','a ','stnd','',''), ('cp','B61U','a ','stnd','',''),
('dm','B61U','a ','stnd','',''), ('ec','B61U','a ','stnd','',''),
('fm','B61U','a ','stnd','',''), ('nt','B61U','a ','stnd','',''),
('qm','B61U','a ','stnd','',''), ('tc','B61U','a ','stnd','',''),
('td','B61U','a ','stnd','',''), ('tf','B61U','a ','stnd','',''),
('tg','B61U','a ','stnd','',''), ('ti','B61U','a ','stnd','',''),
('tp','B61U','a ','stnd','',''), ('ts','B61U','a ','stnd','',''),
('wh','B61U','a ','stnd','','');
show create table t1;

select t_vers,t_rele,t_cust,filler1 from t1 where t_vers = '7.6';
select t_vers,t_rele,t_cust,filler1 from t1 where t_vers = '7.6'
  and t_rele='a' and t_cust = ' ';

drop table t1;

# BUG#19021: Crash in index_merge/ROR-intersection optimizer under
# specific circumstances.
create table t1 (
  pk int(11) not null auto_increment,
  a int(11) not null default '0',
  b int(11) not null default '0',
  c int(11) not null default '0',

  filler1 datetime, filler2 varchar(15),
  filler3 longtext,

  kp1 varchar(4), kp2 varchar(7),
  kp3 varchar(2), kp4 varchar(4),
  kp5 varchar(7),
  filler4 char(1),

  primary key (pk),
  key idx1(a,b,c),
  key idx2(c),
  key idx3(kp1,kp2,kp3,kp4,kp5)
) default charset=latin1;
--disable_query_log
--enable_query_log
SELECT COUNT(*) FROM t1 WHERE b = 0 AND a = 0 AND c = 13286427 AND
 kp1='279' AND kp2='ELM0678' AND kp3='6' AND kp4='10' AND  kp5 = 'R        ';

drop table t1;

# BUG#21277: Index Merge/sort_union: wrong query results
create table t1
(
  key1 int not null, 
  key2 int not null default 0,
  key3 int not null default 0
);

insert into t1(key1) values (1),(2),(3),(4),(5),(6),(7),(8);


alter table t1 add index i2(key2);
alter table t1 add index i3(key3);
update t1 set key2=key1,key3=key1 where key1 is not null;

-- disable_query_log
-- disable_result_log
analyze table t1;
-- enable_result_log
-- enable_query_log

if ($index_merge_random_rows_in_EXPLAIN)
{
  --replace_column 9 #
}
# to test the bug, the following must use "sort_union":
explain select * from t1 where (key3 > 30 and key3<35) or (key2 >32 and key2 < 40);
select * from t1 where (key3 > 30 and key3<35) or (key2 >32 and key2 < 40);
drop table t1;

--echo #
--echo # Bug#56423: Different count with SELECT and CREATE SELECT queries
--echo #

CREATE TABLE t1 (
  a INT,
  b INT,
  c INT,
  d INT,
  PRIMARY KEY (a),
  KEY (c),
  KEY bd (b,d)
);

INSERT INTO t1 VALUES
(1, 0, 1, 0),
(2, 1, 1, 1),
(3, 1, 1, 1),
(4, 0, 1, 1);

-- disable_query_log
-- disable_result_log
analyze table t1;
-- enable_result_log
-- enable_query_log

EXPLAIN
SELECT a
FROM t1
WHERE c = 1 AND b = 1 AND d = 1;

CREATE TABLE t2 ( a INT )
SELECT a
FROM t1
WHERE c = 1 AND b = 1 AND d = 1;

SELECT * FROM t2;

DROP TABLE t1, t2;

CREATE TABLE t1( a INT, b INT, KEY(a), KEY(b) );
INSERT INTO t1 VALUES (1, 2), (1, 2), (1, 2), (1, 2);
SELECT * FROM t1 FORCE INDEX(a, b) WHERE a = 1 AND b = 2;

DROP TABLE t1;

--echo # Code coverage of fix.
CREATE TABLE t1 ( a INT NOT NULL AUTO_INCREMENT PRIMARY KEY, b INT);
INSERT INTO t1 (b) VALUES (1);
UPDATE t1 SET b = 2 WHERE a = 1;
SELECT * FROM t1;

CREATE TABLE t2 ( a INT NOT NULL AUTO_INCREMENT PRIMARY KEY, b VARCHAR(1) );
INSERT INTO t2 (b) VALUES ('a');
UPDATE t2 SET b = 'b' WHERE a = 1;
SELECT * FROM t2;

DROP TABLE t1, t2;

--echo #
--echo # BUG#13970015: ASSERT `MIN_ENDP || MAX_ENDP' FAILED IN
--echo #               HANDLER::MULTI_RANGE_READ_INFO_CONST
--echo #

CREATE TABLE t1 (
  pk INT NOT NULL,
  col_int_key INT NOT NULL,
  col_varchar_key VARCHAR(1) NOT NULL,
  PRIMARY KEY (pk),
  KEY col_int_key (col_int_key),
  KEY col_varchar_key (col_varchar_key,col_int_key)
);

INSERT INTO t1 VALUES (1,1,'a'), (2,2,'b');

-- disable_query_log
-- disable_result_log
analyze table t1;
-- enable_result_log
-- enable_query_log

EXPLAIN
SELECT col_int_key
FROM t1
WHERE col_varchar_key >= 'l' OR 
      (((pk BETWEEN 141 AND 141) OR col_varchar_key <> 'l') 
         AND ((pk BETWEEN 141 AND 141) OR (col_int_key > 141)));

SELECT col_int_key
FROM t1
WHERE col_varchar_key >= 'l' OR 
      (((pk BETWEEN 141 AND 141) OR col_varchar_key <> 'l') 
         AND ((pk BETWEEN 141 AND 141) OR (col_int_key > 141)));

DROP TABLE t1;
