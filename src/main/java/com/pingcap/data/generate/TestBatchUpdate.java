package com.pingcap.data.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestBatchUpdate {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestBatchUpdate.class);
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static Connection connection;

    public static void main(String[] args) {
        parseArgs(args);
        String driver = "com.mysql.jdbc.Driver";
        String url = String.format("jdbc:mysql://%s/test?&rewriteBatchedStatements=true&allowMultiQueries=false&useServerPrepStmts=true&prepStmtCacheSqlLimit=65536&cachePrepStmts=true&zeroDateTimeBehavior=convertToNull&useSSL=false&&useConfigs=maxPerformance", IP_PORT);

        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, USER, PASSWORD);
            Map<Integer, String> paramMap = new HashMap<>();
            paramMap.put(1,"sh1");
            paramMap.put(2,"sh2");
            paramMap.put(3,"sh3");
            executeSQL("update t4 set name = ? where id = ?", paramMap);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void executeSQL(String sql, Map<Integer, String> paramMap) throws Exception {
        LOGGER.info("execute sql : " + sql);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        for (Map.Entry<Integer, String> temp : paramMap.entrySet()) {
            preparedStatement.setString(1, temp.getValue());
            preparedStatement.setInt(2, temp.getKey());
            preparedStatement.addBatch();
        }

        preparedStatement.executeBatch();
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
