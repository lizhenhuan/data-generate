package com.pingcap.data.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;

public class TestTiDBRunSQL {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestTiDBRunSQL.class);
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static String DB;
    private static String SQL;
    private static Connection connection;

    public static void main(String[] args) {
        parseArgs(args);
        String driver = "com.mysql.jdbc.Driver";
        String url = String.format("jdbc:mysql://%s/%s?&zeroDateTimeBehavior=convertToNull&useSSL=false&&useConfigs=maxPerformance&rewriteBatchedStatements=true&allowMultiQueries=true&useServerPrepStmts=true&prepStmtCacheSqlLimit=65536&cachePrepStmts=true&sessionVariables=tidb_isolation_read_engines='tidb,tikv'", IP_PORT, DB);

        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, USER, PASSWORD);
            executeSQL(SQL);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void executeSQL(String sql) throws Exception {
        Long startTime = new Date().getTime();
        LOGGER.info("execute sql : " + sql );
        PreparedStatement preparedStatementCreate = connection.prepareStatement(sql);
        preparedStatementCreate.execute();
        Long endTime = new Date().getTime();
        LOGGER.info("execute sql end cost time : " + (endTime - startTime) /1000 );
    }
    private static void executeQueryShowResult(String sql) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.executeQuery();
        ResultSet resultSet = preparedStatement.getResultSet();
        boolean hasNext = resultSet.next();
        if (!hasNext) {
            LOGGER.info("query result is empty!");
        }
        while (hasNext || resultSet.next()) {
            hasNext = false;
            Long id = resultSet.getLong(1);
            String name = resultSet.getString(2);
            LOGGER.info(String.format("query result is : { id: %d, name: %s }", id , name));
        }
    }
    private static void parseArgs(String [] args) {
        if (args.length == 5) {
            IP_PORT = args[0];
            USER = args[1];
            PASSWORD = args[2];
            DB = args[3];
            SQL = args[4];
        } else {
            LOGGER.error("please input 5 args: IP_PORT USER PASSWORD DB SQL");
            System.exit(1);
        }
    }
}
