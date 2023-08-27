package com.pingcap.data.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class TestTiDB {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestTiDB.class);
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static Connection connection;

    public static void main(String[] args) {
        parseArgs(args);
        String driver = "com.mysql.jdbc.Driver";
        String url = String.format("jdbc:mysql://%s/piaoju?&zeroDateTimeBehavior=convertToNull&useSSL=false&&useConfigs=maxPerformance&rewriteBatchedStatements=true&allowMultiQueries=true&useServerPrepStmts=true&prepStmtCacheSqlLimit=65536&cachePrepStmts=true", IP_PORT);

        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, USER, PASSWORD);
            executeSQL("drop table if exists jdbc_table");
            executeSQL("create table jdbc_table (id bigint primary key, name varchar(32))");
            executeQueryShowResult("select * from jdbc_table");
            executeSQL("insert into jdbc_table values(1,'sh')");
            executeQueryShowResult("select * from jdbc_table");
            executeSQL("update jdbc_table set name = 'bj' where id = 1");
            executeQueryShowResult("select * from jdbc_table");
            executeSQL("delete from jdbc_table");
            executeQueryShowResult("select * from jdbc_table");
            executeSQL("drop table jdbc_table");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void executeSQL(String sql) throws Exception {
        LOGGER.info("execute sql : " + sql);
        PreparedStatement preparedStatementCreate = connection.prepareStatement(sql);
        preparedStatementCreate.execute();
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
        if (args.length == 3) {
            IP_PORT = args[0];
            USER = args[1];
            PASSWORD = args[2];
        } else {
            LOGGER.error("please input 3 args: IP_PORT USER PASSWORD");
            System.exit(1);
        }
    }
}
