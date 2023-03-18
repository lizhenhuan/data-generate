package com.pingcap.data.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class StreamFetch {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamFetch.class);
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static String DB_NAME;
    private static String TABLE_NAME;

    public static void main(String[] args) {
        parseArgs(args);

        String driver = "com.mysql.jdbc.Driver";
        String url = String.format("jdbc:mysql://%s/%s?useUnicode=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true", IP_PORT, DB_NAME);

        try {
            Class.forName(driver);
            String sql = String.format("select * from %s.%s order by k", DB_NAME, TABLE_NAME);
            try (Connection connection = DriverManager.getConnection(url, USER, PASSWORD)) {
                 Statement stmt = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                         java.sql.ResultSet.CONCUR_READ_ONLY);
                 stmt.setFetchSize(Integer.MIN_VALUE);
                 ResultSet resultSet = stmt.executeQuery(sql);
                 ResultSetMetaData metaData = resultSet.getMetaData();
                 int count = metaData.getColumnCount();
                 StringBuilder stringBuilder = new StringBuilder();
                 while (resultSet.next()) {
                     for (int i = 1; i <= count; i++) {
                         stringBuilder.append(metaData.getColumnName(i)).append(":").append(resultSet.getString(i)).append(",");
                     }
                     stringBuilder.append("\n");
                     LOGGER.info(stringBuilder.toString());
                     stringBuilder.setLength(0);
                 }

            } catch (SQLException e) {
                LOGGER.error("error happen here,", e);
            }
        } catch (Exception e) {
            LOGGER.error("error happen here,", e);
        }
    }

    private static void parseArgs(String [] args) {
        if (args.length == 5) {
            IP_PORT = args[0];
            USER = args[1];
            PASSWORD = args[2];
            DB_NAME = args[3];
            TABLE_NAME = args[4];
        } else {
            LOGGER.error("please input 5 args: IP_PORT USER PASSWORD  DB_NAME TABLE_NAME ,but get %s", args.length);
            System.exit(1);
        }
    }
}
