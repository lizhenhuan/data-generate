package com.pingcap.data.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class DataGenerateRelation {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataGenerateRelation.class);
    private static int THREAD_NUM;
    private static Long TOTAL_SIZE;
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static String DB_NAME;
    private static String TABLE_NAME;
    private static String SOURCE_COLUMN;
    private static String RELATION_TABLE;
    private static String RELATION_COLUMN;
    private static AtomicLong atomicLong = new AtomicLong();
    private static Long RELATION_TABLE_COUNT = 0L;


    public static void main(String[] args) {
        parseArgs(args);

        String driver = "com.mysql.jdbc.Driver";
        String url = String.format("jdbc:mysql:loadbalance://%s/%s?useUnicode=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true&sessionVariables=tidb_dml_batch_size=500&sessionVariables=tidb_batch_insert=1", IP_PORT, DB_NAME);

        try {
            Class.forName(driver);
            final CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
            List<Connection> connectionList = new ArrayList<>();
            for (int i = 0; i < THREAD_NUM; i++) {
                Connection connection = DriverManager.getConnection(url, USER, PASSWORD);
                PreparedStatement preparedStatement1 = connection.prepareStatement("set tidb_dml_batch_size=500 ");
                preparedStatement1.execute();
                PreparedStatement preparedStatement2 = connection.prepareStatement("set tidb_batch_insert=1 ");
                preparedStatement2.execute();
                connectionList.add(connection);
            }
            List<ColumnInfo> columnInfoList = MetaUtil.getMeta(connectionList.get(0), DB_NAME, TABLE_NAME);
            String insertSQL = MetaUtil.generateRelationInsertSQL(columnInfoList, DB_NAME, TABLE_NAME, SOURCE_COLUMN, RELATION_TABLE, RELATION_COLUMN);
            LOGGER.info("insert SQL:" + insertSQL);
            RELATION_TABLE_COUNT = getRelationTableCount(connectionList.get(0), RELATION_TABLE);
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
        try (PreparedStatement ps = connection.prepareStatement(sql);) {
            for (int count = 0; count < TOTAL_SIZE / THREAD_NUM / RELATION_TABLE_COUNT; count ++) {
                ps.execute();
                atomicLong.addAndGet(RELATION_TABLE_COUNT);
                LOGGER.info(String.format("insert into %s.%s count %s， total count: %s", DB_NAME, TABLE_NAME, RELATION_TABLE_COUNT, atomicLong.get()));
            }
        }
    }

    private static Long getRelationTableCount(Connection connection, String relationTable) throws Exception {
        String sql = "select count(1) from " + relationTable;
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery();) {
            while (rs.next()) {
                return rs.getLong(1);
            }
        }
        throw new Exception("get relation table count error");
    }

    private static void parseArgs(String [] args) {
        if (args.length == 10) {
            THREAD_NUM = Integer.parseInt(args[0]);
            TOTAL_SIZE = Long.parseLong(args[1]);
            IP_PORT = args[2];
            USER = args[3];
            PASSWORD = args[4];
            DB_NAME = args[5];
            TABLE_NAME = args[6];
            SOURCE_COLUMN = args[7];
            RELATION_TABLE = args[8];
            RELATION_COLUMN = args[9];
        } else {
            LOGGER.error("please input 9 args: THREAD_NUM TOTAL_SIZE  IP_PORT USER PASSWORD  DB_NAME TABLE_NAME SOURCE_COLUMN RELATION_TABLE RELATION_COLUMN ");
            System.exit(1);
        }
    }
}

