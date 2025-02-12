package com.pingcap.data.generate;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataGenerate {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataGenerate.class);
    private static int THREAD_NUM;
    private static Long TOTAL_SIZE;
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static String DB_NAME;
    private static String TABLE_NAME;
    private static int BATCH_SIZE = 500;
    private static AtomicLong atomicLong = new AtomicLong();

    public static void main(String[] args) {
        parseArgs(args);

        String driver = "com.mysql.jdbc.Driver";
        String url = String.format("jdbc:mysql://%s/%s?useUnicode=true&useSSL=false&useServerPrepStmts=true&prepStmtCacheSqlLimit=655360000&prepStmtCacheSize=2000&allowMultiQueries=true&cachePrepStmts=true&rewriteBatchedStatements=true&useConfigs=maxPerformance", IP_PORT, DB_NAME);

        try {
            Class.forName(driver);
            final CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
            List<Connection> connectionList = new ArrayList<>();
            for (int i = 0; i < THREAD_NUM; i++) {
                Connection connection = DriverManager.getConnection(url, USER, PASSWORD);
                connectionList.add(connection);
            }
            List<ColumnInfo> columnInfoList = MetaUtil.getMeta(connectionList.get(0), DB_NAME, TABLE_NAME);
            String insertSQL = MetaUtil.generateInsertSQL(columnInfoList, DB_NAME, TABLE_NAME);
            LOGGER.info("insert SQL:" + insertSQL);

            Long start = System.currentTimeMillis();
            for (Connection connection : connectionList) {
                executorService.execute(
                        () -> {
                            try {
                                insertData(connection, insertSQL, columnInfoList);
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
    public static void insertData(Connection connection, String sql, List<ColumnInfo> columnInfoList) throws Exception {
        Random random = new Random();
        try (PreparedStatement ps =  connection.prepareStatement(sql);) {
            for (int count = 0; count < TOTAL_SIZE / THREAD_NUM / BATCH_SIZE; count ++) {
                for (int i = 0; i < BATCH_SIZE; i++) {
                    for (int j = 0; j < columnInfoList.size(); j++) {
                        columnInfoList.get(j).setPreparedStatement(ps, j + 1, random);
                    }
                    ps.addBatch();
                }
                int[] countArr = ps.executeBatch();
                atomicLong.addAndGet(countArr.length);
                LOGGER.info(String.format("insert into %s.%s count %s， total count: %s", DB_NAME, TABLE_NAME, countArr.length, atomicLong.get()));
            }
        }
    }

    private static void parseArgs(String [] args) {
        if (args.length == 7 || args.length == 8) {
            THREAD_NUM = Integer.parseInt(args[0]);
            TOTAL_SIZE = Long.parseLong(args[1]);
            IP_PORT = args[2];
            USER = args[3];
            PASSWORD = args[4];
            DB_NAME = args[5];
            TABLE_NAME = args[6];
            if (args.length >= 8) {
                BATCH_SIZE = Integer.parseInt(args[7]);
            }
        } else {
            LOGGER.error("please input 7 args: THREAD_NUM TOTAL_SIZE  IP_PORT USER PASSWORD  DB_NAME TABLE_NAME (BATCH_SIZE) ");
            System.exit(1);
        }
    }
}

