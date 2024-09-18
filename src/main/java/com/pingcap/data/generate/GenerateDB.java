//package com.pingcap.data.generate;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
public class GenerateDB {
//    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateDB.class);
//    private static int THREAD_NUM;
//    private static Long TOTAL_SIZE;
//    private static String IP_PORT;
//    private static String USER;
//    private static String PASSWORD;
//    private static String DB_NAME;
//    private static int BATCH_SIZE = 500;
//
//    public static void main(String[] args) {
//        parseArgs(args);
//        String driver = "com.mysql.jdbc.Driver";
//        String url = String.format("jdbc:mysql://%s/%s?useUnicode=true&useSSL=false&useServerPrepStmts=true&prepStmtCacheSqlLimit=655360&prepStmtCacheSize=2000&allowMultiQueries=true&cachePrepStmts=true&rewriteBatchedStatements=true&useConfigs=maxPerformance", IP_PORT, DB_NAME);
//
//        try {
//            Class.forName(driver);
//            final CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
//            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
//            List<Connection> connectionList = new ArrayList<>();
//            Connection connection = null;
//            for (int i = 0; i < THREAD_NUM; i++) {
//                connection = DriverManager.getConnection(url, USER, PASSWORD);
//                connectionList.add(connection);
//            }
//            connection.prepareStatement("show tables in " + DB_NAME);
//
//            List<ColumnInfo> columnInfoList = MetaUtil.getMeta(connectionList.get(0), DB_NAME, TABLE_NAME);
//            String insertSQL = MetaUtil.generateInsertSQL(columnInfoList, DB_NAME, TABLE_NAME);
//            LOGGER.info("insert SQL:" + insertSQL);
//
//            Long start = System.currentTimeMillis();
//            for (Connection connection : connectionList) {
//                executorService.execute(
//                        () -> {
//                            try {
//                                DataGenerate.insertData(connection, insertSQL, columnInfoList);
//                            } catch (Exception e) {
//                                LOGGER.error("send message error", e);
//                            } finally {
//                                countDownLatch.countDown();
//                                try {
//                                    connection.close();
//                                } catch (Exception e) {
//                                }
//                            }
//                        }
//                );
//            }
//            countDownLatch.await();
//            Long end = System.currentTimeMillis();
//            LOGGER.info("cost time:" + (end - start) / 1000.0 + " s");
//            LOGGER.info("QPS is:" + TOTAL_SIZE /((end-start)/1000.0));
//            System.exit(0);
//        } catch (Exception e) {
//            LOGGER.error("error happen here,", e);
//        }
//    }
//
//
//
//    private static void parseArgs(String [] args) {
//        if (args.length == 6 || args.length == 7) {
//            THREAD_NUM = Integer.parseInt(args[0]);
//            TOTAL_SIZE = Long.parseLong(args[1]);
//            IP_PORT = args[2];
//            USER = args[3];
//            PASSWORD = args[4];
//            DB_NAME = args[5];
//            if (args.length >= 7) {
//                BATCH_SIZE = Integer.parseInt(args[6]);
//            }
//        } else {
//            LOGGER.error("please input 7 args: THREAD_NUM TOTAL_SIZE  IP_PORT USER PASSWORD  DB_NAME (BATCH_SIZE) ");
//            System.exit(1);
//        }
//    }
}
