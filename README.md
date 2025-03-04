Generate data into mysql or tidb by input table name .
This project will query table information schema and column info by table name to generate insert sql, and generate random number , varchar , date value to insert into target table . Insert sql will skip auto_increment or auto_random column.


Build from source:

mvn clean package

This command will generate target/data-generate-1.0-SNAPSHOT.jar


Generate data to mysql or tidb

java -Xms2048m -Xmx2048m -cp ~/data-generate-1.0-SNAPSHOT.jar com.pingcap.data.generate.DataGenerate THREAD_NUM TOTAL_SIZE  IP_PORT USER PASSWORD  DB_NAME TABLE_NAME (BATCH_SIZE)

BATCH_SIZE is  optional with default value 500 

Example :

java -cp target/data-generate-1.0-SNAPSHOT.jar com.pingcap.data.generate.DataGenerate 10 10000 172.1.1.1:4000 root 'xxx!@#' test t1 500

TOTAL_SIZE show bigger than THREAD_NUM * BATCH_SIZE(500) otherwise will skip the for loop and insert nothing 


Generate relation table data:

first generate main table data:

java -cp target/data-generate-1.0-SNAPSHOT.jar com.pingcap.data.generate.DataGenerate 10 10000 172.1.1.1:4000 root 'xxx!@#' test t_user

then generate relation table by main table:

java -cp target/data-generate-1.0-SNAPSHOT.jar com.pingcap.data.generate.DataGenerateRelation THREAD_NUM TOTAL_SIZE  IP_PORT USER PASSWORD  DB_NAME TABLE_NAME SOURCE_COLUMN RELATION_TABLE RELATION_COLUMN
java -cp target/data-generate-1.0-SNAPSHOT.jar com.pingcap.data.generate.DataGenerateRelation 10 2000000 172.16.4.137:4000 root 'xxx!@#' test t_order u_id t_user id


Generate table data by insert insert sql:
java -cp target/data-generate-1.0-SNAPSHOT.jar com.pingcap.data.generate.DataGenerateBySQL THREAD_NUM TOTAL_SIZE  IP_PORT USER PASSWORD  DB_NAME SQL ONCE_INSERT_COUNT


Test Currency DDL:
create database test1;
create database test2;
create database test3;
create database test4;


drop table test1.employees;
drop table test2.employees;
drop table test3.employees;
drop table test4.employees;

CREATE TABLE test1.employees (
id int unsigned NOT NULL,
fname varchar(30),
lname varchar(30),
hired date NOT NULL DEFAULT '1970-01-01',
separated date DEFAULT '9999-12-31',
job_code int,
store_id int NOT NULL
) PARTITION BY RANGE (id)
INTERVAL (100) FIRST PARTITION LESS THAN (100) LAST PARTITION LESS THAN (10000) MAXVALUE PARTITION;

CREATE TABLE test2.employees (
id int unsigned NOT NULL,
fname varchar(30),
lname varchar(30),
hired date NOT NULL DEFAULT '1970-01-01',
separated date DEFAULT '9999-12-31',
job_code int,
store_id int NOT NULL
) PARTITION BY RANGE (id)
INTERVAL (100) FIRST PARTITION LESS THAN (100) LAST PARTITION LESS THAN (10000) MAXVALUE PARTITION;



CREATE TABLE test4.employees (
id int unsigned NOT NULL,
fname varchar(30),
lname varchar(30),
hired date NOT NULL DEFAULT '1970-01-01',
separated date DEFAULT '9999-12-31',
job_code int,
store_id int NOT NULL
) PARTITION BY RANGE (id) INTERVAL(100) FIRST PARTITION LESS THAN (100) LAST PARTITION LESS THAN (10000) MAXVALUE PARTITION;


CREATE TABLE test3.employees (
id int unsigned NOT NULL,
fname varchar(30),
lname varchar(30),
hired date NOT NULL DEFAULT '1970-01-01',
separated date DEFAULT '9999-12-31',
job_code int,
store_id int NOT NULL
) PARTITION BY RANGE (id) INTERVAL (100) FIRST PARTITION LESS THAN (100) LAST PARTITION LESS THAN (10000) MAXVALUE PARTITION;

run TestCurrencyDDL:
java -cp target/data-generate-1.0-SNAPSHOT.jar com.pingcap.data.generate.TestCurrencyDDL  THREAD_NUM  IP_PORT USER PASSWORD  DB_NAME
example: java -cp target/data-generate-1.0-SNAPSHOT.jar com.pingcap.data.generate.TestCurrencyDDL  4 127.0.0.1:4000 root root test
