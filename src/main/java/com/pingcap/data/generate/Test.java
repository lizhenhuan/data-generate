package com.pingcap.data.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class Test {
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static String DB_NAME;

    private static final Logger LOGGER = LoggerFactory.getLogger(Test.class);
    public static void main(String[] args) throws Exception {
        parseArgs(args);
        generate();
    }

    public static void generate() throws Exception {
        String[][] databases = {
                {"jdbc:mysql://abcd/abcd?useUnicode=true&characterEncoding=utf-8&useSSL=false","abcd","abcd"},
                {"jdbc:mysql://abcd/abcd?useUnicode=true&characterEncoding=utf-8&useSSL=false","abcd","abcd"},
        };

        for (int i = 0; i < databases.length; i++) {
            String url = String.format("jdbc:mysql://%s/%s?useUnicode=true&useSSL=false&useServerPrepStmts=true&prepStmtCacheSqlLimit=655360&prepStmtCacheSize=2000&allowMultiQueries=true&cachePrepStmts=true&rewriteBatchedStatements=true&useConfigs=maxPerformance", IP_PORT, DB_NAME);
            Connection connection = DriverManager.getConnection(url, USER, PASSWORD);

            ResultSet rs =connection.prepareStatement("show variables like '%version%'").executeQuery();
            LOGGER.info("=================Version===================");
            while (rs.next()){
                LOGGER.info("{}={}",rs.getString(1),rs.getString(2));
            }
            LOGGER.info("=================ACT_RU_EXECUTION===================");

            rs = connection.getMetaData().getTables(null,null,"ACT_RU_EXECUTION",new String []{"TABLE"});
            while (rs.next()){
                LOGGER.info("{}.{}",rs.getString(1),rs.getString(3));
            }

        }
    }
    private static void parseArgs(String [] args) {
        if (args.length == 4 ) {
            IP_PORT = args[0];
            USER = args[1];
            PASSWORD = args[2];
            DB_NAME = args[3];
        } else {
            LOGGER.error("please input 3 args:  IP_PORT USER PASSWORD  DB_NAME  ");
            System.exit(1);
        }
    }
}
