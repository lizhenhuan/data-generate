package com.pingcap.data.generate;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class MetaUtil {
    private static final String QUERY_COLUMN = "select column_name,data_type,COLUMN_KEY,EXTRA,CHARACTER_MAXIMUM_LENGTH,IS_NULLABLE from information_schema.COLUMNS where table_schema='%s' and table_name = '%s'";
    private static final String QUERY_CREATE_TABLE = "show create table %s.%s";
    public static void main(String[] args) {
        System.out.println(UUID.randomUUID().toString().length());
    }

    public static List<ColumnInfo> getMeta(Connection connection, String dbName, String tableName) throws Exception {
        List<ColumnInfo> columnInfoList = new ArrayList<>();
        try (PreparedStatement ps =  connection.prepareStatement(String.format(QUERY_COLUMN, dbName, tableName));
             PreparedStatement psCreateTable =  connection.prepareStatement(String.format(QUERY_CREATE_TABLE, dbName, tableName));
             ResultSet rs = ps.executeQuery();
             ResultSet rsCreateTable = psCreateTable.executeQuery()) {
                boolean isAutoRandom = false;
                while (rsCreateTable.next()) {
                    String createTableSQL = rsCreateTable.getString(2);
                    if (createTableSQL.contains("[auto_rand]")) {
                        isAutoRandom = true;
                    }
                }
                while (rs.next()) {
                    ColumnInfo columnInfo = convert(rs);
                    // auto_increment or auto_random should not add to list
                    if (("PRI".equals(columnInfo.getColumnKey()) && isAutoRandom) || "auto_increment".equals(columnInfo.getExtra())) {
                        continue;
                    } else {
                        columnInfoList.add(columnInfo);
                    }
                }
        }
        return columnInfoList;
    }

//    public static List<String> getDBTables(Connection connection, String dbName) throws Exception {
//        List<String> tableNameList = new ArrayList<>();
//        try (PreparedStatement ps =  connection.prepareStatement(String.format(QUERY_COLUMN, dbName, tableName));
//             PreparedStatement psCreateTable =  connection.prepareStatement(String.format(QUERY_CREATE_TABLE, dbName, tableName));
//             ResultSet rs = ps.executeQuery();
//             ResultSet rsCreateTable = psCreateTable.executeQuery()) {
//            boolean isAutoRandom = false;
//            while (rsCreateTable.next()) {
//                String createTableSQL = rsCreateTable.getString(2);
//                if (createTableSQL.contains("[auto_rand]")) {
//                    isAutoRandom = true;
//                }
//            }
//            while (rs.next()) {
//                ColumnInfo columnInfo = convert(rs);
//                // auto_increment or auto_random should not add to list
//                if (("PRI".equals(columnInfo.getColumnKey()) && isAutoRandom) || "auto_increment".equals(columnInfo.getExtra())) {
//                    continue;
//                } else {
//                    columnInfoList.add(columnInfo);
//                }
//            }
//        }
//        return columnInfoList;
//    }

    public static String generateInsertSQL(List<ColumnInfo> columnInfoList, String dbName, String tableName) {
        StringBuilder stringBuilder = new StringBuilder(String.format("insert into %s.%s (", dbName, tableName));
        StringBuilder stringBuilder2 = new StringBuilder();
        for (ColumnInfo columnInfo : columnInfoList) {
            if (!"auto_increment".equals(columnInfo.getExtra())) {
                stringBuilder.append("`").append(columnInfo.getColumnName()).append("`,");
                stringBuilder2.append("?,");
            }
        }
        stringBuilder.setLength(stringBuilder.length() - 1);
        stringBuilder2.setLength(stringBuilder2.length() - 1);
        stringBuilder.append(") values (");
        stringBuilder2.append(")");
        return stringBuilder.append(stringBuilder2).toString();
    }

    public static String generateReplaceSQL(List<ColumnInfo> columnInfoList, String dbName, String tableName) {
        StringBuilder stringBuilder = new StringBuilder(String.format("replace into %s.%s (", dbName, tableName));
        StringBuilder stringBuilder2 = new StringBuilder();
        for (ColumnInfo columnInfo : columnInfoList) {
            if (!"auto_increment".equals(columnInfo.getExtra())) {
                stringBuilder.append("`").append(columnInfo.getColumnName()).append("`,");
                stringBuilder2.append("?,");
            }
        }
        stringBuilder.setLength(stringBuilder.length() - 1);
        stringBuilder2.setLength(stringBuilder2.length() - 1);
        stringBuilder.append(") values (");
        stringBuilder2.append(")");
        return stringBuilder.append(stringBuilder2).toString();
    }

    public static String generateRelationInsertSQL(List<ColumnInfo> columnInfoList, String dbName, String tableName, String sourceColumn, String relationTable, String relationColumn) throws Exception {
        StringBuilder stringBuilder = new StringBuilder(String.format("insert into %s.%s ( ", dbName, tableName));
        StringBuilder stringBuilderSelect = new StringBuilder(" select ");
        for (ColumnInfo columnInfo : columnInfoList) {
            stringBuilder.append(columnInfo.getColumnName()).append(",");
            stringBuilderSelect.append(columnInfo.generateRelationColumnSQL(sourceColumn, relationTable, relationColumn));
        }
        stringBuilder.setLength(stringBuilder.length() - 1);
        stringBuilderSelect.setLength(stringBuilderSelect.length() -1);
        stringBuilder.append(" ) ");
        stringBuilderSelect.append(" from ").append(relationTable);

        return stringBuilder.append(stringBuilderSelect).toString();
    }


    private static ColumnInfo convert(ResultSet rs) throws Exception {

        return new ColumnInfo(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getLong(5), rs.getString(6));
    }




}
