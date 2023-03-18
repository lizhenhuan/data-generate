package com.pingcap.data.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class TestSessionVariable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestSessionVariable.class);

    public static void main(String[] args) throws Exception {
        String driver = "com.mysql.jdbc.Driver";
        String url = String.format("jdbc:mysql:loadbalance://172.31.2.165:4000/anker?useUnicode=true&characterEncoding=utf-8&useSSL=false&rewriteBatchedStatements=true&sessionVariables=tidb_dml_batch_size=500&sessionVariables=tidb_batch_insert=1");
        Class.forName(driver);
        Connection connection = DriverManager.getConnection(url, "root", "pingcap@");
        PreparedStatement preparedStatement1 = connection.prepareStatement("set tidb_dml_batch_size=500 ");
        preparedStatement1.execute();
        PreparedStatement preparedStatement2 = connection.prepareStatement("set tidb_batch_insert=1 ");
        preparedStatement2.execute();
        PreparedStatement preparedStatement = connection.prepareStatement("show variables like '%batch%' ");
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            LOGGER.info(String.format("key is %s, value is %s",resultSet.getString(1), resultSet.getString(2)));
        }


    }
}
