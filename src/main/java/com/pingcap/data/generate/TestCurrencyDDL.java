package com.pingcap.data.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class TestCurrencyDDL {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestCurrencyDDL.class);
    private static int THREAD_NUM;
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static String DB_NAME;


    public static void main(String[] args) {
        parseArgs(args);

        String driver = "com.mysql.jdbc.Driver";


        try {
            Class.forName(driver);
            final CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
            List<Connection> connectionList = new ArrayList<>();
            for (int i = 1; i <= THREAD_NUM; i++) {
                String dbName = DB_NAME + i;
                String url = String.format("jdbc:mysql://%s/%s?useUnicode=true&useSSL=false&useServerPrepStmts=true&prepStmtCacheSqlLimit=655360000&prepStmtCacheSize=2000&allowMultiQueries=true&cachePrepStmts=true&rewriteBatchedStatements=true&useConfigs=maxPerformance", IP_PORT, dbName);
                Connection connection = DriverManager.getConnection(url, USER, PASSWORD);
                connectionList.add(connection);
            }


            Long start = System.currentTimeMillis();
            for (Connection connection : connectionList) {
                executorService.execute(
                        () -> {
                            try {
                                LOGGER.info("start start");
                                runDDL(connection);
                            } catch (Exception e) {
                                LOGGER.error("send message error", e);
                            } finally {
                                countDownLatch.countDown();
                                try {
                                    connection.close();
                                } catch (Exception e) {
                                }
                            }
                        }
                );
            }
            countDownLatch.await();
            Long end = System.currentTimeMillis();
            LOGGER.info("cost time:" + (end - start) / 1000.0 + " s");
            System.exit(0);
        } catch (Exception e) {
            LOGGER.error("error happen here,", e);
        }
    }
    public static void runDDL(Connection connection) throws Exception {
        for (int count = 100; count < 2100; count = count + 100) {
            Long start = System.currentTimeMillis();
            try (PreparedStatement ps =  connection.prepareStatement("alter table employees truncate partition P_LT_" + count);) {
                ps.execute();
            }
            Long end = System.currentTimeMillis();
            LOGGER.info("truncate table partition P_LT_" + count + " cost time :" + (end - start));
        }
    }

    private static void parseArgs(String [] args) {
        if (args.length == 5) {
            THREAD_NUM = Integer.parseInt(args[0]);
            IP_PORT = args[1];
            USER = args[2];
            PASSWORD = args[3];
            DB_NAME = args[4];
        } else {
            LOGGER.error("please input 5 args: THREAD_NUM  IP_PORT USER PASSWORD  DB_NAME ");
            System.exit(1);
        }
    }
}

