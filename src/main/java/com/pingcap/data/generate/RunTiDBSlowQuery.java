package com.pingcap.data.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RunTiDBSlowQuery {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestTiDBRunSQL.class);
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static String DB;
    private static String TIDB_IP_PORT;
    private static String TIDB_USER;
    private static String TIDB_PASSWORD;
    private static String SQL = "select time,source_sql,db,id from run_slow_query_api where db in ('account','basic','charge','cif','crius','currency','eventcenter','fof','marketing','merchant','payment','product','risk','test','transcore','xts','xxl_job') order by time asc ";
    private static Connection connection;
    private static Connection connectionTiDB;
    private static int count = 0;


    public static void main(String[] args) {
        parseArgs(args);
        String driver = "com.mysql.cj.jdbc.Driver";
        String url = String.format("jdbc:mysql://%s/%s?&zeroDateTimeBehavior=convertToNull&useSSL=false&&useConfigs=maxPerformance&rewriteBatchedStatements=true&allowMultiQueries=true&useServerPrepStmts=true&prepStmtCacheSqlLimit=65536&cachePrepStmts=true", IP_PORT, DB);
        String tidbURL = String.format("jdbc:mysql://%s/%s?&zeroDateTimeBehavior=convertToNull&useSSL=false&&useConfigs=maxPerformance&rewriteBatchedStatements=true&allowMultiQueries=true&useServerPrepStmts=true&prepStmtCacheSqlLimit=65536&cachePrepStmts=true", TIDB_IP_PORT, "test");
        long startTime = 0;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, USER, PASSWORD);
            connectionTiDB = DriverManager.getConnection(tidbURL, TIDB_USER, TIDB_PASSWORD);
            List<SlowQuery> sqlList = executeQuery(SQL);
            String currentDB = "";
            String runSQL = "";
            startTime = System.currentTimeMillis();
            for (SlowQuery slowQuery:sqlList) {

                // Define regex pattern to match the arguments part
                String sql = slowQuery.getSourceSQL();
                Pattern pattern = Pattern.compile("\\[arguments: \\(?(.*?)\\)?\\]");
                Matcher matcher = pattern.matcher(sql);
                runSQL = sql.replaceAll("\\[arguments: \\(?(.*?)\\)?\\]","");
                if (!slowQuery.getDb().equals(currentDB)) {
                    currentDB = slowQuery.getDb();
                    try(PreparedStatement statementUse = connectionTiDB.prepareStatement("use " + currentDB)) {
                        statementUse.execute();
                    }
                }

                try (PreparedStatement statement = connectionTiDB.prepareStatement(runSQL)) {
                    // Extract and display the arguments
                    if (matcher.find()) {
                        String argumentsString = matcher.group(1);


                        // Parse the extracted arguments
                        String[] arguments = parseArguments(argumentsString);

                        int i = 1;
                        for (String argument : arguments) {
                            statement.setString(i++, argument);
                        }

                    }
                    statement.execute();
                    count++;
//                    try (PreparedStatement statementSet = connectionTiDB.prepareStatement("update test.run_slow_query set status = 1 where id = ?");){
//                        statementSet.setInt(1,slowQuery.getId());
//                        statementSet.execute();
//                    }
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
//                    try (PreparedStatement statementSet = connectionTiDB.prepareStatement("update test.run_slow_query set status = 0 where id = ?");){
//                        statementSet.setInt(1,slowQuery.getId());
//                        statementSet.execute();
//                    }
                }
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        long endTime = System.currentTimeMillis();
        LOGGER.info("end, count is :" + count + "; cost time:" + (endTime - startTime) );
    }

    private static String[] parseArguments(String argumentsString) {
        // Split by comma but ignore commas inside quotes
        return argumentsString.split(", (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
    }

    private static List<SlowQuery> executeQuery(String sql) throws Exception {

        List<SlowQuery> sqlList = new ArrayList<>();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.executeQuery();
        ResultSet resultSet = preparedStatement.getResultSet();
        boolean hasNext = resultSet.next();
        if (!hasNext) {
            LOGGER.info("query result is empty!");
        }
        while (hasNext || resultSet.next()) {
            hasNext = false;
            String sourceSQL = resultSet.getString(2);
            String db = resultSet.getString(3);
            int id = resultSet.getInt(4);
            SlowQuery slowQuery = new SlowQuery(sourceSQL, db, id);
            sqlList.add(slowQuery);
        }
        LOGGER.info("size is :" + sqlList.size());
        return sqlList;
    }
    private static void parseArgs(String [] args) {
        if (args.length == 7) {
            IP_PORT = args[0];
            USER = args[1];
            PASSWORD = args[2];
            DB = args[3];
            TIDB_IP_PORT = args[4];
            TIDB_USER = args[5];
            TIDB_PASSWORD = args[6];
        } else {
            LOGGER.error("please input 6 args: IP_PORT USER PASSWORD DB TIDB_IP_PORT TIDB_USER TIDB_PASSWORD");
            System.exit(1);
        }
    }

    static class SlowQuery {
        private String sourceSQL;
        private String db;
        private int id;

        public SlowQuery(String sourceSQL, String db, int id) {
            this.sourceSQL = sourceSQL;
            this.db = db;
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getSourceSQL() {
            return sourceSQL;
        }

        public void setSourceSQL(String sourceSQL) {
            this.sourceSQL = sourceSQL;
        }

        public String getDb() {
            return db;
        }

        public void setDb(String db) {
            this.db = db;
        }
    }

}
