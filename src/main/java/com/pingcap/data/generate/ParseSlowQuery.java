//package com.pingcap.data.generate;
//
//import net.sf.jsqlparser.util.TablesNamesFinder;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileReader;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Locale;
//import java.util.Set;
//
//public class ParseSlowQuery {
//    private static final Logger LOGGER = LoggerFactory.getLogger(ParseSlowQuery.class);
//
//    private static String SLOW_QUERY_DIR;
//    public static void main(String[] args) {
//        parseArgs(args);
//        File[] files = new File(SLOW_QUERY_DIR).listFiles();
//        List<SlowQuery> slowQueryList = new ArrayList<>();
//        for (File file : files) {
//            if (file.isFile()) {
//                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
//                    String line = reader.readLine();
//                    SlowQuery slowQuery = new SlowQuery();
//                    while (line != null) {
//
//                        if (line.startsWith("# Time: ")) {
//                            slowQuery = new SlowQuery();
//                            slowQuery.setTime(line.replace("# Time: ", ""));
//                        } else if (line.startsWith("# Is_internal: ")) {
//                            slowQuery.setIsInternal(line.replace("# Is_internal: ", ""));
//                        } else if (line.startsWith("# User@Host: ")) {
//                            slowQuery.setIsInternal(line.replace("# Is_internal: ", ""));
//                        } else if (line.startsWith("# Query_time: ")) {
//                            slowQuery.setIsInternal(line.replace("# Is_internal: ", ""));
//                        } else if (line.startsWith("# DB: ")) {
//                            slowQuery.setDb(line.replace("# DB: ", ""));
//                        } else if (line.startsWith("# Prepared: ")) {
//                            slowQuery.setIsInternal(line.replace("# Is_internal: ", ""));
//                        } else if (line.startsWith("# Backoff_total: ")) {
//                            slowQuery.setIsInternal(line.replace("# Is_internal: ", ""));
//                        } else if (line.startsWith("# Plan_from_cache: ")) {
//                            slowQuery.setIsInternal(line.replace("# Is_internal: ", ""));
//                        } else if (line.toLowerCase(Locale.ROOT).startsWith("select") || line.toLowerCase(Locale.ROOT).startsWith("insert") || line.toLowerCase(Locale.ROOT).startsWith("update") || line.toLowerCase(Locale.ROOT).startsWith("delete") || line.toLowerCase(Locale.ROOT).startsWith("set") || line.toLowerCase(Locale.ROOT).startsWith("drop")) {
//                            System.out.println(line);
//                            slowQuery.setSql(line);
//                            Set<String> tableNames = TablesNamesFinder.findTables(line);
//                            slowQuery.setTableNames(tableNames);
//                            System.out.println(tableNames);
//                            slowQueryList.add(slowQuery);
//                        }
//                        line = reader.readLine();
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        for (SlowQuery slowQuery : slowQueryList) {
//            System.out.println(slowQuery.getTableNames() + " ======= " + slowQuery.getSql());
//        }
//        System.out.println(slowQueryList.size());
//
//    }
//    private static void parseArgs(String [] args) {
//        if (args.length == 1) {
//            SLOW_QUERY_DIR = args[0];
//        } else {
//            LOGGER.error("please input 1 args: SLOW_QUERY_DIR");
//            System.exit(1);
//        }
//    }
//
//    private static class SlowQuery {
//        private String time;
//        private String user;
//        private String queryTime;
//        private String isInternal;
//        private String prepared;
//        private String planFromCache;
//        private String BackoffTotal;
//        private String sql;
//        private Set<String> tableNames;
//        private String db;
//
//        public String getDb() {
//            return db;
//        }
//
//        public void setDb(String db) {
//            this.db = db;
//        }
//
//        public Set<String> getTableNames() {
//            return tableNames;
//        }
//
//        public void setTableNames(Set<String> tableNames) {
//            this.tableNames = tableNames;
//        }
//
//        public String getTime() {
//            return time;
//        }
//
//        public void setTime(String time) {
//            this.time = time;
//        }
//
//        public String getUser() {
//            return user;
//        }
//
//        public void setUser(String user) {
//            this.user = user;
//        }
//
//        public String getQueryTime() {
//            return queryTime;
//        }
//
//        public void setQueryTime(String queryTime) {
//            this.queryTime = queryTime;
//        }
//
//        public String getIsInternal() {
//            return isInternal;
//        }
//
//        public void setIsInternal(String isInternal) {
//            this.isInternal = isInternal;
//        }
//
//        public String getPrepared() {
//            return prepared;
//        }
//
//        public void setPrepared(String prepared) {
//            this.prepared = prepared;
//        }
//
//        public String getPlanFromCache() {
//            return planFromCache;
//        }
//
//        public void setPlanFromCache(String planFromCache) {
//            this.planFromCache = planFromCache;
//        }
//
//        public String getBackoffTotal() {
//            return BackoffTotal;
//        }
//
//        public void setBackoffTotal(String backoffTotal) {
//            BackoffTotal = backoffTotal;
//        }
//
//        public String getSql() {
//            return sql;
//        }
//
//        public void setSql(String sql) {
//            this.sql = sql;
//        }
//    }
//}
