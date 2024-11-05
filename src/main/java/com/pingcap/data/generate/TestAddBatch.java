package com.pingcap.data.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Date;

public class TestAddBatch {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTiDB.class);
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static String DB;
    private static int BATCH_SIZE;
    private static Connection connection;

    public static void main(String[] args) {
        parseArgs(args);
        String driver = "com.mysql.jdbc.Driver";
        String url = String.format("jdbc:mysql://%s/%s?&zeroDateTimeBehavior=convertToNull&useSSL=false&&useConfigs=maxPerformance&rewriteBatchedStatements=true&allowMultiQueries=true&useServerPrepStmts=true&prepStmtCacheSqlLimit=2147483640&cachePrepStmts=true&prepStmtCacheSize=2000", IP_PORT, DB);

        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, USER, PASSWORD);


            executeBatchReplaceSQL(BATCH_SIZE);
            executeBatchReplaceSQL(BATCH_SIZE);
            executeBatchReplaceSQL(BATCH_SIZE);
            executeBatchReplaceSQL(BATCH_SIZE);


            //executeReplaceSQL();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void executeBatchInsertSQL() throws Exception {
        Long startTime = new Date().getTime();
        String sql = "insert into t values(?,?)";
        LOGGER.info("execute sql : " + sql );
        PreparedStatement preparedStatementCreate = connection.prepareStatement(sql);
        for (int i = 0; i < 100; i++) {
            preparedStatementCreate.setInt(1,i);
            preparedStatementCreate.setString(2,i + "str");
            preparedStatementCreate.addBatch();
        }
        preparedStatementCreate.executeBatch();
        Long endTime = new Date().getTime();
        LOGGER.info("execute sql end cost time : " + (endTime - startTime)  );
    }

    private static void executeReplaceSQL() throws Exception {
        Long startTime = new Date().getTime();
        String sql = "replace into t values(?,?)";
        LOGGER.info("execute sql : " + sql);
        try (PreparedStatement preparedStatementCreate = connection.prepareStatement(sql);) {
            for (int i = 100; i < 101; i++) {
                preparedStatementCreate.setInt(1, i);
                preparedStatementCreate.setString(2, i + "str");
                preparedStatementCreate.execute();
            }
        }
        Long endTime = new Date().getTime();
        LOGGER.info("execute sql end cost time : " + (endTime - startTime) );
    }

    private static void executeBatchReplaceSQL(int count) throws Exception {
        Long startTime = new Date().getTime();
        String sql = "replace into t values(?,?)";
        LOGGER.info("execute sql : " + sql);
        PreparedStatement preparedStatementCreate = connection.prepareStatement(sql);
        for (int i = 0; i < count; i++) {
            preparedStatementCreate.setInt(1,i);
            preparedStatementCreate.setString(2,i + "str");
            preparedStatementCreate.addBatch();
        }
        preparedStatementCreate.executeBatch();
        Long endTime = new Date().getTime();
        LOGGER.info("execute sql end cost time : " + (endTime - startTime) );
    }

    private static void parseArgs(String [] args) {
        if (args.length == 5) {
            IP_PORT = args[0];
            USER = args[1];
            PASSWORD = args[2];
            DB = args[3];
            BATCH_SIZE = Integer.parseInt(args[4]);
        } else {
            LOGGER.error("please input 5 args: IP_PORT USER PASSWORD DB SQL");
            System.exit(1);
        }
    }
}
