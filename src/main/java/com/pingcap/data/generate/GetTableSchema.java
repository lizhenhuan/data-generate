package com.pingcap.data.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class GetTableSchema {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetTableSchema.class);
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static String DB_NAME;
    private static Connection connection;

    public static void main(String[] args) {
        parseArgs(args);
        String driver = "com.mysql.jdbc.Driver";
        String url = String.format("jdbc:mysql://%s/mysql?&zeroDateTimeBehavior=convertToNull&useSSL=false&&useConfigs=maxPerformance&rewriteBatchedStatements=true&allowMultiQueries=true&useServerPrepStmts=true&prepStmtCacheSqlLimit=65536&cachePrepStmts=true", IP_PORT);

        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, USER, PASSWORD);
            String [] dbNames = DB_NAME.split(",");
            for (String dbName : dbNames) {
                showDBCreateTable(dbName);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void showDBCreateTable(String db) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement("show tables in " + db);
        preparedStatement.executeQuery();
        ResultSet resultSet = preparedStatement.getResultSet();
        boolean hasNext = resultSet.next();
        if (!hasNext) {
            LOGGER.info("query result is empty!");
        }
        while (hasNext || resultSet.next()) {
            hasNext = false;
            String tableName = resultSet.getString(1);

            PreparedStatement showCreateTableStatement = connection.prepareStatement(String.format("show create table  %s.%s", db, tableName));
            showCreateTableStatement.executeQuery();
            ResultSet showCreateTableResultSet = showCreateTableStatement.getResultSet();
            while (showCreateTableResultSet.next()) {
                String showCreateTableSQL = showCreateTableResultSet.getString(2);
                LOGGER.info(showCreateTableSQL + ";\n");
            }
            LOGGER.info("\n");
        }
    }
    private static void parseArgs(String [] args) {
        if (args.length == 4) {
            IP_PORT = args[0];
            USER = args[1];
            PASSWORD = args[2];
            DB_NAME = args[3];
        } else {
            LOGGER.error("please input 4 args: IP_PORT USER PASSWORD DB_NAME");
            System.exit(1);
        }
    }
}
