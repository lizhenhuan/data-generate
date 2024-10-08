package com.pingcap.data.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;

public class TestOracleRunSQL {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestOracleRunSQL.class);
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static String DB;
    private static String SQL;
    private static Connection connection;


    public static void main(String[] args) {
        parseArgs(args);
        String driver = "oracle.jdbc.OracleDriver";
        String url = String.format("jdbc:oracle:thin:@172.xx.xx.xx:1521:xxdb");

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
        if (args.length == 3) {
            USER = args[0];
            PASSWORD = args[1];
            SQL = args[2];
            System.out.println(String.format("%s + %s + %s",USER, PASSWORD, SQL));
        } else {
            LOGGER.error("please input 3 args:  USER PASSWORD  SQL");
            System.exit(1);
        }
    }
}
