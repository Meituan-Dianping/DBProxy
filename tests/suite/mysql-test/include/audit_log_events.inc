# produce some events for audit log

CREATE TABLE t1 (c1 INT, c2 CHAR(20));
--error ER_TABLE_EXISTS_ERROR
CREATE TABLE t1
             (c1 INT,
              c2 CHAR(20));
INSERT INTO t1 VALUES (1,'a'),(2,'b'),(3,'c');
SELECT * FROM t1;
--error ER_NO_SUCH_TABLE
SELECT * FROM t2;
DROP TABLE t1;






show variables like 'audit_log%';
--replace_result $MASTER_MYSOCK MASTER_SOCKET $MASTER_MYPORT MASTER_PORT
--error ER_ACCESS_DENIED_ERROR
create table t1 (id int);
create table t2 (id int);
insert into t1 values (1), (2);
insert into t2 values (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2), (1), (2);
select * from t1;
alter table t1 rename renamed_t1;
--error ER_NO_SUCH_TABLE
select * from t_doesnt_exist;
--error 1064
syntax_error_query;
drop table renamed_t1, t2;
show variables like 'audit_log%';
create database sa_db;
create table t1 (id2 int);
insert into t1 values (1), (2);
select * from t1;
drop table t1;
create table sa_t1(id int);
insert into sa_t1 values (1), (2);
drop table sa_t1;
drop database sa_db;
select '&;&&&""""<><<>>>>';
