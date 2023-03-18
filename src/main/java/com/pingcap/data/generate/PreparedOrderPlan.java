package com.pingcap.data.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PreparedOrderPlan {

    private static final Logger LOGGER = LoggerFactory.getLogger(PreparedOrderPlan.class);
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static String TABLE_NAME = "oc_b_occupy_task";

    public static void main(String[] args) {
        parseArgs(args);

        String driver = "com.mysql.jdbc.Driver";
        String url = String.format("jdbc:mysql://%s/pre_r3_order?&zeroDateTimeBehavior=convertToNull&useSSL=false&&useConfigs=maxPerformance&rewriteBatchedStatements=true&allowMultiQueries=true&useServerPrepStmts=true&prepStmtCacheSqlLimit=65536&cachePrepStmts=true", IP_PORT);

        try {
            Class.forName(driver);
            Connection connection = DriverManager.getConnection(url, USER, PASSWORD);
            String sql0 = "select 1";
            String sql1 = String.format(" explain plan UPDATE pre_r3_order.%s SET STATUS = 1,modifieddate = NOW() WHERE order_id IN (?,?,?,?,?)", TABLE_NAME);
            String sql2 = String.format(" explain plan UPDATE pre_r3_order.%s SET STATUS = 2,modifieddate = NOW() WHERE order_id IN (?,?,?,?,?,?)", TABLE_NAME);
            String sql3 = String.format(" explain plan UPDATE pre_r3_order.%s SET STATUS = 3,modifieddate = NOW() WHERE order_id IN (?,?,?,?)", TABLE_NAME);
            String sql4 = String.format(" explain plan UPDATE pre_r3_order.%s SET STATUS = 4,modifieddate = NOW() WHERE order_id IN (?,?,?,?)", TABLE_NAME);
            String sql5 = String.format(" explain plan UPDATE pre_r3_order.%s SET STATUS = 5,modifieddate = NOW() WHERE order_id IN (?,?,?,?)", TABLE_NAME);
            String sql6 = String.format(" explain plan UPDATE pre_r3_order.%s SET STATUS = 6,modifieddate = NOW() WHERE order_id IN (113034457, 113034458, 113034460, 113031239, 113031241)", TABLE_NAME);

            // 113033460, 113033461, 113033462, 113034452, 113034456, 113034457, 113034458, 113034460, 113031239, 113031241,
            try (PreparedStatement ps0 =  connection.prepareStatement(sql0);
                 PreparedStatement ps1 =  connection.prepareStatement(sql1);
                 PreparedStatement ps2 =  connection.prepareStatement(sql2);
                 PreparedStatement ps3 =  connection.prepareStatement(sql3);
                 PreparedStatement ps4 =  connection.prepareStatement(sql4);
                 PreparedStatement ps5 =  connection.prepareStatement(sql5);
                 PreparedStatement ps6 =  connection.prepareStatement(sql6)) {
                ps1.setInt(1,113033460);
                ps1.setInt(2,113033461);
                ps1.setInt(3,113033462);
                ps1.setInt(4,113034452);
                ps1.setInt(5,113034456);

                ps2.setString(1,"113034457");
                ps2.setString(2,"113034458");
                ps2.setString(3,"113034460");
                ps2.setString(4,"113031239");
                ps2.setString(5,"113031241");
                ps2.setString(6,"113031242");

                //  113031242, 113031243, 113031244, 113032633, 113032634,
                ps3.setDouble(1,113033460.0);
                ps3.setDouble(2,113032634.0);
                ps3.setDouble(3,113032633.0);
                ps3.setDouble(4,113031244.0);

                ps4.setBigDecimal(1,new BigDecimal(113033460));
                ps4.setBigDecimal(2,new BigDecimal(113033460));
                ps4.setBigDecimal(3,new BigDecimal(113032633));
                ps4.setBigDecimal(4,new BigDecimal(113031244));

                // caputure

                ps5.setLong(1,new Long(113033460));
                ps5.setLong(2,new Long(113032633));
                ps5.setLong(3,new Long(113031244));
                ps5.setLong(4,new Long(113031241));


                Long start = System.currentTimeMillis();
                outputPlan(ps0);
                Long end0 = System.currentTimeMillis();
                LOGGER.info("ps0 cost:" + (end0 - start));
                outputPlan(ps1);
                Long end1 = System.currentTimeMillis();
                LOGGER.info("ps1 cost:" + (end1 - end0));
                outputPlan(ps2);
                Long end2 = System.currentTimeMillis();
                LOGGER.info("ps2 cost:" + (end2 - end1));
                outputPlan(ps3);
                Long end3 = System.currentTimeMillis();
                LOGGER.info("ps3 cost:" + (end3 - end2));
                outputPlan(ps4);
                Long end4 = System.currentTimeMillis();
                LOGGER.info("ps4 cost:" + (end4 - end3));
                outputPlan(ps5);
                Long end5 = System.currentTimeMillis();
                LOGGER.info("ps5 cost:" + (end5 - end4));
                outputPlan(ps6);
                Long end6 = System.currentTimeMillis();
                LOGGER.info("ps6 cost:" + (end6 - end5));
            }
        } catch (Exception e) {
            LOGGER.error("error happen here,", e);
        }

    }

    private static void outputPlan(PreparedStatement ps) throws Exception {
        try (ResultSet resultSet = ps.executeQuery()){
            while (resultSet.next()) {
                LOGGER.info(resultSet.getString(1));
            }
        }
    }

    private static void parseArgs(String [] args) {
        if (args.length >= 3) {
            IP_PORT = args[0];
            USER = args[1];
            PASSWORD = args[2];
            if (args.length > 3) {
                TABLE_NAME = args[3];
            }
        } else {
            LOGGER.error("please input 4 args: IP_PORT USER PASSWORD TABLE_NAME");
            System.exit(1);
        }
    }
}
