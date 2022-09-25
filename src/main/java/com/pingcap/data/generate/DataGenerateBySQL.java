package com.pingcap.data.generate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataGenerateBySQL {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataGenerateBySQL.class);
    private static int THREAD_NUM;
    private static Long TOTAL_SIZE;
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static String DB_NAME;
    private static String SQL;
    private static int ONCE_INSERT_COUNT;
    private static AtomicLong atomicLong = new AtomicLong();

    public static void main(String[] args) {
        parseArgs(args);

        String driver = "com.mysql.jdbc.Driver";
        String url = String.format("jdbc:mysql:loadbalance://%s/%s?useUnicode=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true", IP_PORT, DB_NAME);

        try {
            Class.forName(driver);
            final CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
            List<Connection> connectionList = new ArrayList<>();
            for (int i = 0; i < THREAD_NUM; i++) {
                Connection connection = DriverManager.getConnection(url, USER, PASSWORD);
                connectionList.add(connection);
            }
            String insertSQL = SQL;
            LOGGER.info("insert SQL:" + insertSQL);

            Long start = System.currentTimeMillis();
            for (Connection connection : connectionList) {
                executorService.execute(
                        () -> {
                            try {
                                insertData(connection, insertSQL);
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
            LOGGER.info("QPS is:" + TOTAL_SIZE /((end-start)/1000.0));
            System.exit(0);
        } catch (Exception e) {
            LOGGER.error("error happen here,", e);
        }
    }
    private static void insertData(Connection connection, String sql) throws Exception {
        try (PreparedStatement ps =  connection.prepareStatement(sql);) {
            for (int count = 0; count < TOTAL_SIZE / THREAD_NUM / ONCE_INSERT_COUNT; count ++) {
                ps.execute();
                atomicLong.addAndGet(ONCE_INSERT_COUNT);
                LOGGER.info(String.format("execute input SQL, once insert count: %s， total count: %s", ONCE_INSERT_COUNT, atomicLong.get()));
            }
        }
    }

    private static void parseArgs(String [] args) {
        if (args.length == 8) {
            THREAD_NUM = Integer.parseInt(args[0]);
            TOTAL_SIZE = Long.parseLong(args[1]);
            IP_PORT = args[2];
            USER = args[3];
            PASSWORD = args[4];
            DB_NAME = args[5];
            SQL = args[6];
            ONCE_INSERT_COUNT = Integer.parseInt(args[7]);
        } else {
            LOGGER.error("please input 7 args: THREAD_NUM TOTAL_SIZE  IP_PORT USER PASSWORD  SQL ONCE_INSERT_COUNT ");
            System.exit(1);
        }
    }
}

