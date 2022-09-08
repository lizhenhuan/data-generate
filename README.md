Generate data into mysql or tidb by input table name .
This project will query table information schema and column info by table name to generate insert sql, and generate random number , varchar , date value to insert into target table . Insert sql will skip auto_increment or auto_random column.


Build from source:

mvn clean package

This command will generate target/data-generate-1.0-SNAPSHOT.jar


Generate data to mysql or tidb

java -Xms2048m -Xmx2048m -cp ~/data-generate-1.0-SNAPSHOT.jar com.pingcap.data.generate.DataGenerate THREAD_NUM TOTAL_SIZE  IP_PORT USER PASSWORD  DB_NAME TABLE_NAME 

Example :
java -cp target/data-generate-1.0-SNAPSHOT.jar com.pingcap.data.generate.DataGenerate 10 10000 172.1.1.1:4000 root 'xxx!@#' test t1

TOTAL_SIZE show bigger than THREAD_NUM * BATCH_SIZE(500) otherwise will skip the for loop and insert nothing 



