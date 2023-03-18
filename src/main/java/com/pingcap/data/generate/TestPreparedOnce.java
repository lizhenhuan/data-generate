package com.pingcap.data.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class TestPreparedOnce {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataGenerate.class);
    private static int THREAD_NUM;
    private static AtomicInteger TOTAL_SIZE = new AtomicInteger();
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static String DB_NAME;
    private static final String SQL = "SELECT A.YYB, A.KHXM, A.KHQZ, A.ZJKZSX, B.ZJZH, B.BZ, B.XTLX, B.JDBH,         B.JYZT, B.LXJS, B.DZLX, B.TZJS, B.SRYE, B.ZHYE, B.JZRQ, B.ZHSX, B.ZHZT, B.ZHLB, B.CGLB, A.JGBZ, B.KHRQ, B.XHRQ, B.LLMB, B.ZZHBZ,          B.ZCXZ, B.ZZJZH, B.JJZCXZ, B.QSRZJE, B.SRZCXZ, B.QSRZRQ FROM tFC_KHZJKZ A, tFC_ZJZH B WHERE A.KHH = B.KHH AND B.KHH = ?";
    public static void main(String[] args) throws Exception {
        parseArgs(args);
        List<String> data = new TestPreparedOnce().getTestData();
        System.out.println(data.size());

        String driver = "com.mysql.jdbc.Driver";
        String url = String.format("jdbc:mysql://%s/%s?useUnicode=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true&useServerPrepStmts=true&cachePrepStmts=true&useConfigs=maxPerformance", IP_PORT, DB_NAME);

        try {
            Class.forName(driver);
            final CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
            List<Connection> connectionList = new ArrayList<>();
            for (int i = 0; i < THREAD_NUM; i++) {
                Connection connection = DriverManager.getConnection(url, USER, PASSWORD);
                connectionList.add(connection);
            }


            Long start = System.currentTimeMillis();
            int index = 0;
            for (Connection connection : connectionList) {
                index++;
                int finalIndex = index;
                executorService.execute(
                        () -> {
                            try {
                                query(connection, data, finalIndex);
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
            new QPSThread().start();
            countDownLatch.await();
            Long end = System.currentTimeMillis();
            LOGGER.info("cost time:" + (end - start) / 1000.0 + " s");
            LOGGER.info("QPS is:" + TOTAL_SIZE.get() /((end-start)/1000.0));
            System.exit(0);
        } catch (Exception e) {
            LOGGER.error("error happen here,", e);
        }
    }
    static  class QPSThread extends Thread {
        public void run() {
            try {
                while (true) {
                    int last = TOTAL_SIZE.get();
                    Thread.sleep(1000);
                    LOGGER.info("QPS is:" + (TOTAL_SIZE.get() - last));
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    private static void query(Connection connection, List<String> data, int index) throws Exception {
        int start = data.size() / THREAD_NUM * (index - 1);
        int end = data.size() / THREAD_NUM * index;
        try (PreparedStatement ps =  connection.prepareStatement(SQL)) {
            for (int i = start; i < end; i++) {
                ps.setString(1, data.get(i));
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {

                }
                TOTAL_SIZE.addAndGet(1);
            }
        }
    }

    private List<String> getTestData() throws IOException {
        InputStream fileInPath = this.getClass().getResourceAsStream("/data.txt");
        List<String> data = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader((InputStream) fileInPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                data.add(line);
            }
        }
        return data;
    }

    private static void parseArgs(String [] args) {
        if (args.length == 5) {
            THREAD_NUM = Integer.parseInt(args[0]);
            IP_PORT = args[1];
            USER = args[2];
            PASSWORD = args[3];
            DB_NAME = args[4];
        } else {
            LOGGER.error("please input 5 args: THREAD_NUM IP_PORT USER PASSWORD DB_NAME ");
            System.exit(1);
        }
    }
}
