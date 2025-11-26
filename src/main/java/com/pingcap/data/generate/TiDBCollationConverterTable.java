package com.pingcap.data.generate;// TiDBCollationConverter.java

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TiDBCollationConverterTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TiDBCollationConverterTable.class);
    // 数据库连接配置，请按实际情况修改
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static String DB_NAME;
    // 目标字符集和排序规则
    private static String TARGET_CHARSET = "utf8mb4";
    private static String TARGET_COLLATION = "utf8mb4_general_ci";

    static class ColumnInfo {
        String tableSchema;
        String tableName;
        String columnName;
        String dataType;
        Long characterMaximumLength;
        String columnDefault;
        String isNullable;
        String columnComment;

        ColumnInfo(String tableSchema, String tableName, String columnName, String dataType,
                   Long characterMaximumLength, String columnDefault, String isNullable, String columnComment) {
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.columnName = columnName;
            this.dataType = dataType;
            this.characterMaximumLength = characterMaximumLength;
            this.columnDefault = columnDefault;
            this.isNullable = isNullable;
            this.columnComment = columnComment;
        }
    }

    static class IndexInfo {
        String tableName;
        String indexName;
        boolean nonUnique;
        String indexType;
        List<String> columnNames; // 用于处理复合索引

        IndexInfo(String tableName, String indexName, boolean nonUnique, String indexType) {
            this.tableName = tableName;
            this.indexName = indexName;
            this.nonUnique = nonUnique;
            this.indexType = indexType;
            this.columnNames = new ArrayList<>();
        }
    }
    private static void parseArgs(String [] args) {
        if (args.length >= 4) {
            IP_PORT = args[0];
            USER = args[1];
            PASSWORD = args[2];
            DB_NAME = args[3];
            if (args.length == 6) {
                TARGET_CHARSET = args[4];
                TARGET_COLLATION = args[5];
            }
        } else {
            LOGGER.error("please input 4 args: IP_PORT USER PASSWORD DB_NAME (TARGET_CHARSET) (TARGET_COLLATION)");
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        parseArgs(args);
        String url = String.format("jdbc:mysql://%s/%s", IP_PORT, DB_NAME);
        try (Connection conn = DriverManager.getConnection(url, USER, PASSWORD)) {
            // 获取需要转换的字段
            List<ColumnInfo> columnsToConvert = findColumnsToConvert(conn);
            // 执行转换
            convertColumnsCharsetAndCollation(conn, columnsToConvert);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static List<ColumnInfo> findColumnsToConvert(Connection conn) throws SQLException {
        List<ColumnInfo> columnInfos = new ArrayList<>();
        // 查询需要转换的字符类型的列
        String sql =
            "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, " +
                   "COLUMN_DEFAULT, IS_NULLABLE, COLUMN_COMMENT " +
            "FROM information_schema.COLUMNS " +
            "WHERE TABLE_SCHEMA = DATABASE() " +
            "AND DATA_TYPE IN ('varchar', 'char', 'text', 'tinytext', 'mediumtext', 'longtext') " +
            "AND (CHARACTER_SET_NAME IS NULL OR CHARACTER_SET_NAME != ? OR COLLATION_NAME IS NULL OR COLLATION_NAME != ?)";
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, TARGET_CHARSET);
            stmt.setString(2, TARGET_COLLATION);
            ResultSet rs = stmt.executeQuery();
            
            while (rs.next()) {
                ColumnInfo info = new ColumnInfo(
                    rs.getString("TABLE_SCHEMA"),
                    rs.getString("TABLE_NAME"),
                    rs.getString("COLUMN_NAME"),
                    rs.getString("DATA_TYPE"),
                    rs.getLong("CHARACTER_MAXIMUM_LENGTH"),
                    rs.getString("COLUMN_DEFAULT"),
                    rs.getString("IS_NULLABLE"),
                    rs.getString("COLUMN_COMMENT")
                );
                columnInfos.add(info);
                System.out.println("Found column to convert: " + info.tableName + "." + info.columnName);
            }
        }
        return columnInfos;
    }

    private static void convertColumnsCharsetAndCollation(Connection conn, List<ColumnInfo> columnsToConvert) throws SQLException {
        conn.setAutoCommit(false); // 开始事务

        try {
            // 按表分组处理列
            Map<String, List<ColumnInfo>> columnsByTable = new HashMap<>();
            for (ColumnInfo columnInfo : columnsToConvert) {
                columnsByTable.computeIfAbsent(columnInfo.tableName, k -> new ArrayList<>()).add(columnInfo);
            }

            for (Map.Entry<String, List<ColumnInfo>> entry : columnsByTable.entrySet()) {
                String tableName = entry.getKey();
                List<ColumnInfo> columns = entry.getValue();

                // 1. 收集需要删除的索引
                Map<String, IndexInfo> indexesToDrop = new HashMap<>();
                for (ColumnInfo columnInfo : columns) {
                    List<IndexInfo> indexesOnColumn = findIndexesOnColumn(conn, tableName, columnInfo.columnName);
                    for (IndexInfo indexInfo : indexesOnColumn) {
                        // 使用索引名作为键，避免重复处理同一个索引
                        indexesToDrop.putIfAbsent(indexInfo.indexName, indexInfo);
                    }
                }

                // 2. 删除索引
                for (IndexInfo indexInfo : indexesToDrop.values()) {
                    if ("PRIMARY".equals(indexInfo.indexName)) {
                        System.out.println("Skipping PRIMARY KEY index on table " + tableName + ", please handle it manually if needed.");
                        continue;
                    }
                    String dropIndexSql = String.format("DROP INDEX `%s` ON `%s`", indexInfo.indexName, tableName);
                    try (Statement stmt = conn.createStatement()) {
                        System.out.println("Executing: " + dropIndexSql);
                        stmt.execute(dropIndexSql);
                    } catch (SQLException e) {
                        System.err.println("Error dropping index " + indexInfo.indexName + " on " + tableName + ": " + e.getMessage());
                        // 根据需求决定是否抛出异常
                    }
                }

                // 3. 修改字段字符集和排序规则
                for (ColumnInfo columnInfo : columns) {
                    String modifyColumnSql = buildModifyColumnSql(columnInfo);
                    try (Statement stmt = conn.createStatement()) {
                        System.out.println("Executing: " + modifyColumnSql);
                        stmt.execute(modifyColumnSql);
                    } catch (SQLException e) {
                        System.err.println("Error modifying column " + tableName + "." + columnInfo.columnName + ": " + e.getMessage());
                    }
                }

                // 4. 重新创建索引
                for (IndexInfo indexInfo : indexesToDrop.values()) {
                    if ("PRIMARY".equals(indexInfo.indexName)) {
                        continue; // 主键索引已跳过
                    }
                    String createIndexSql = buildCreateIndexSql(indexInfo);
                    try (Statement stmt = conn.createStatement()) {
                        System.out.println("Executing: " + createIndexSql);
                        stmt.execute(createIndexSql);
                    } catch (SQLException e) {
                        System.err.println("Error recreating index " + indexInfo.indexName + " on " + tableName + ": " + e.getMessage());
                    }
                }
                try (Statement stmt = conn.createStatement()) {
                    String alterTableCHARSETSQL =  String.format("alter table %s CHARSET=%s", tableName, TARGET_CHARSET);
                    String alterTableCOLLATESQL =  String.format("alter table %s COLLATE=%s", tableName, TARGET_COLLATION);

                    System.out.println(alterTableCHARSETSQL);
                    System.out.println(alterTableCOLLATESQL);

                    stmt.execute(alterTableCHARSETSQL);
                    stmt.execute(alterTableCOLLATESQL);
                } catch (SQLException e) {
                    System.err.println("Error alter table " + tableName + " COLLATE " +  ": " + e.getMessage());
                }
            }
            conn.commit();
            System.out.println("Charset and collation conversion completed successfully.");
        } catch (SQLException e) {
            System.err.println("Conversion failed, rolling back: " + e.getMessage());
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
    }

    private static List<IndexInfo> findIndexesOnColumn(Connection conn, String tableName, String columnName) throws SQLException {
        List<IndexInfo> indexInfos = new ArrayList<>();
        // 查询指定表和列上的索引信息 (不包括主键)
        String sql =
            "SELECT INDEX_NAME, NON_UNIQUE, INDEX_TYPE " +
            "FROM information_schema.STATISTICS " +
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ? AND INDEX_NAME != 'PRIMARY'";
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, tableName);
            stmt.setString(2, columnName);
            ResultSet rs = stmt.executeQuery();
            
            while (rs.next()) {
                IndexInfo info = new IndexInfo(
                    tableName,
                    rs.getString("INDEX_NAME"),
                    rs.getBoolean("NON_UNIQUE"),
                    rs.getString("INDEX_TYPE")
                );
                // 获取索引涉及的列（用于复合索引）
                getIndexColumns(conn, tableName, info);
                indexInfos.add(info);
            }
        }
        return indexInfos;
    }

    private static void getIndexColumns(Connection conn, String tableName, IndexInfo indexInfo) throws SQLException {
        String sql =
            "SELECT COLUMN_NAME, SUB_PART " +
            "FROM information_schema.STATISTICS " +
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ? " +
            "ORDER BY SEQ_IN_INDEX";
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, tableName);
            stmt.setString(2, indexInfo.indexName);
            ResultSet rs = stmt.executeQuery();
            
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                Long subPart = rs.getLong("SUB_PART"); // 前缀索引长度
                if (subPart != null && subPart > 0) {
                    columnName += "(" + subPart + ")";
                }
                indexInfo.columnNames.add(columnName);
            }
        }
    }

    private static String buildModifyColumnSql(ColumnInfo columnInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE `").append(columnInfo.tableName).append("` MODIFY COLUMN `").append(columnInfo.columnName).append("` ");
        sb.append(columnInfo.dataType);
        // 处理长度限制
        if (columnInfo.characterMaximumLength != null && columnInfo.characterMaximumLength > 0) {
            sb.append("(").append(columnInfo.characterMaximumLength).append(")");
        }
        sb.append(" CHARACTER SET ").append(TARGET_CHARSET);
        sb.append(" COLLATE ").append(TARGET_COLLATION);
        // 处理默认值、可空性等属性
        if ("NO".equals(columnInfo.isNullable)) {
            sb.append(" NOT NULL");
        } else {
            sb.append(" NULL");
        }
        if (columnInfo.columnDefault != null) {
            sb.append(" DEFAULT '").append(columnInfo.columnDefault).append("'");
        }
        if (columnInfo.columnComment != null && !columnInfo.columnComment.isEmpty()) {
            sb.append(" COMMENT '").append(columnInfo.columnComment).append("'");
        }
        return sb.toString();
    }

    private static String buildCreateIndexSql(IndexInfo indexInfo) {
        StringBuilder sb = new StringBuilder();
        if (indexInfo.nonUnique) {
            sb.append("CREATE INDEX ");
        } else {
            sb.append("CREATE UNIQUE INDEX ");
        }
        sb.append("`").append(indexInfo.indexName).append("` ON `").append(indexInfo.tableName).append("` (");
        sb.append(String.join(", ", indexInfo.columnNames));
        sb.append(")");
        // 可以添加其他索引选项，如索引类型 (USING BTREE) 等
        return sb.toString();
    }
}