build from source:

mvn clean package


generate data to mysql or tidb

java -Xms2048m -Xmx2048m -cp ~/data-generate-1.0-SNAPSHOT.jar com.pingcap.data.generate.DataGenerate THREAD_NUM TOTAL_SIZE  IP_PORT USER PASSWORD  DB_NAME TABLE_NAME 

example :
java -cp target/data-generate-1.0-SNAPSHOT.jar com.pingcap.data.generate.DataGenerate 10 10000 172.1.1.1:4000 root 'xxx!@#' test t1

TOTAL_SIZE show bigger than THREAD_NUM * BATCH_SIZE(500) otherwise will skip the for loop and insert nothing 



