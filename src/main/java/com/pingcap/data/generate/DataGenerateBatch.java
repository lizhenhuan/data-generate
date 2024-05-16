package com.pingcap.data.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class DataGenerateBatch {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataGenerateBatch.class);
    private static int THREAD_NUM;
    private static Long TOTAL_SIZE;
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static String DB_NAME;
    private static int BATCH_SIZE = 500;
    private static AtomicLong atomicLong = new AtomicLong();
    private static String [] TABLES = {"bl_blwdjl",
            "fy_mjz_ghsfjl",
            "fy_zy_jsjl",
            "hl_tz_tzxx",
            "hz_gm_gmxx",
            "hz_jzjbxx",
            "jc_bgzb",
            "jcbl_bgzb",
            "jy_bgzb",
            "jy_wswbgzb",
            "jz_mjz_jzjl",
            "jz_mjz_zdjl",
            "jz_zy_zdjl",
            "jz_zy_zyjl",
            "ss_ssjl",
            "sx_sqdjl",
            "sx_sxjl",
            "tj_tjbgwd",
            "tj_tjdjxx",
            "yp_mz_fyjl",
            "yz_mjz_fypcfxx",
            "yz_mjz_ypcfxx",
            "yz_zy_fypyzxx",
            "yz_zy_ypyzxx"};

    public static void main(String[] args) {
        parseArgs(args);

        String driver = "com.mysql.jdbc.Driver";
        String url = String.format("jdbc:mysql://%s/%s?useUnicode=true&useSSL=false&useServerPrepStmts=true&prepStmtCacheSqlLimit=655360&prepStmtCacheSize=2000&allowMultiQueries=true&cachePrepStmts=true&rewriteBatchedStatements=true&useConfigs=maxPerformance", IP_PORT, DB_NAME);

        try {
            Class.forName(driver);
            THREAD_NUM = THREAD_NUM * TABLES.length;
            final CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);

            List<TableMetaInfo> tableMetaInfoList = new ArrayList<>();
            for (int i = 0; i < THREAD_NUM; i++) {
                Connection connection = DriverManager.getConnection(url, USER, PASSWORD);
                List<ColumnInfo> columnInfoList = MetaUtil.getMeta(connection, DB_NAME, TABLES[i % TABLES.length]);
                String insertSQL = MetaUtil.generateInsertSQL(columnInfoList, DB_NAME, TABLES[i % TABLES.length]);
                LOGGER.info("insert SQL:" + insertSQL);
                tableMetaInfoList.add(new TableMetaInfo(columnInfoList, insertSQL, connection));
            }

            Long start = System.currentTimeMillis();
            for (TableMetaInfo tableMetaInfo : tableMetaInfoList) {
                executorService.execute(
                        () -> {
                            try {
                                insertData(tableMetaInfo.getConnection(), tableMetaInfo.getInsertSQL(), tableMetaInfo.getColumnInfoList());
                            } catch (Exception e) {
                                LOGGER.error("send message error", e);
                            } finally {
                                countDownLatch.countDown();
                                try {
                                    tableMetaInfo.getConnection().close();
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
    private static void insertData(Connection connection, String sql, List<ColumnInfo> columnInfoList) throws Exception {
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
                LOGGER.info(String.format("insert into batch tables count %s， total count: %s", TABLES.length, atomicLong.get()));
            }
        }
    }

    private static void parseArgs(String [] args) {
        if (args.length == 6 || args.length == 7) {
            THREAD_NUM = Integer.parseInt(args[0]);
            TOTAL_SIZE = Long.parseLong(args[1]);
            IP_PORT = args[2];
            USER = args[3];
            PASSWORD = args[4];
            DB_NAME = args[5];
            if (args.length >= 7) {
                BATCH_SIZE = Integer.parseInt(args[6]);
            }
        } else {
            LOGGER.error("please input 7 args: SINGLE_TABLE_THREAD_NUM TOTAL_SIZE  IP_PORT USER PASSWORD  DB_NAME (BATCH_SIZE) ");
            System.exit(1);
        }
    }
}

