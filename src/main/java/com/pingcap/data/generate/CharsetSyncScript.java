package com.pingcap.data.generate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class CharsetSyncScript {
    private static final Logger LOGGER = LoggerFactory.getLogger(CharsetSyncScript.class);


    // 数据库连接配置，请按实际情况修改
    private static String MYSQL_IP_PORT;
    private static String MYSQL_USER;
    private static String MYSQL_PASSWORD;
    private static String MYSQL_DB_NAME;

    private static String TIDB_IP_PORT;
    private static String TIDB_USER;
    private static String TIDB_PASSWORD;
    private static String TIDB_DB_NAME;

    private static Set<String> PRIMARY_KEY_SQL_SET = new HashSet();



    // 存储表信息的内部类
    static class TableInfo {
        String tableSchema;
        String tableName;
        String sourceCharset;
        String sourceCollation;
        String targetCharset;
        String targetCollation;

        TableInfo(String tableSchema, String tableName, String sourceCharset, String sourceCollation,
                  String targetCharset, String targetCollation) {
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.sourceCharset = sourceCharset;
            this.sourceCollation = sourceCollation;
            this.targetCharset = targetCharset;
            this.targetCollation = targetCollation;
        }
    }

    // 存储字段信息的内部类
    static class ColumnInfo {
        String tableSchema;
        String tableName;
        String columnName;
        String dataType;
        Long characterMaximumLength;
        String sourceCharset;
        String sourceCollation;
        String targetCharset;
        String targetCollation;
        String columnDefault;
        String isNullable;
        String columnComment;

        ColumnInfo(String tableSchema, String tableName, String columnName, String dataType,
                   Long characterMaximumLength, String sourceCharset, String sourceCollation,
                   String targetCharset, String targetCollation, String columnDefault,
                   String isNullable, String columnComment) {
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.columnName = columnName;
            this.dataType = dataType;
            this.characterMaximumLength = characterMaximumLength;
            this.sourceCharset = sourceCharset;
            this.sourceCollation = sourceCollation;
            this.targetCharset = targetCharset;
            this.targetCollation = targetCollation;
            this.columnDefault = columnDefault;
            this.isNullable = isNullable;
            this.columnComment = columnComment;
        }
    }

    // 存储索引信息的内部类
    static class IndexInfo {
        String tableName;
        String indexName;
        boolean nonUnique;
        String indexType;
        List<String> columnNames; // 用于处理复合索引
        List<Long> subParts; // 前缀索引长度

        IndexInfo(String tableName, String indexName, boolean nonUnique, String indexType) {
            this.tableName = tableName;
            this.indexName = indexName;
            this.nonUnique = nonUnique;
            this.indexType = indexType;
            this.columnNames = new ArrayList<>();
            this.subParts = new ArrayList<>();
        }
    }

    private static void parseArgs(String [] args) {
        if (args.length == 8) {
            MYSQL_IP_PORT = args[0];
            MYSQL_USER = args[1];
            MYSQL_PASSWORD = args[2];
            MYSQL_DB_NAME = args[3];

            TIDB_IP_PORT = args[4];
            TIDB_USER = args[5];
            TIDB_PASSWORD = args[6];
            TIDB_DB_NAME = args[7];

        } else {
            LOGGER.error("please input 8 args: MYSQL_IP_PORT MYSQL_USER MYSQL_PASSWORD MYSQL_DB_NAME TIDB_IP_PORT TIDB_USER TIDB_PASSWORD TIDB_DB_NAME");
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        parseArgs(args);
        String sourceMySQLUrl = String.format("jdbc:mysql://%s/%s", MYSQL_IP_PORT, MYSQL_DB_NAME);
        String targetTiDBUrl = String.format("jdbc:mysql://%s/%s", TIDB_IP_PORT, TIDB_DB_NAME);
        Connection sourceConn = null;
        Connection targetConn = null;
        
        try {
            // 1. 建立数据库连接
            sourceConn = DriverManager.getConnection(sourceMySQLUrl, MYSQL_USER, MYSQL_PASSWORD);
            targetConn = DriverManager.getConnection(targetTiDBUrl, TIDB_USER, TIDB_PASSWORD);

            // 2. 查找需要同步的表和字段
            List<TableInfo> tablesToSync = findTablesToSync(sourceConn, targetConn);
            List<ColumnInfo> columnsToSync = findColumnsToSync(sourceConn, targetConn);

            if (tablesToSync.isEmpty() && columnsToSync.isEmpty()) {
                System.out.println("上下游字符集和排序规则已一致，无需同步。");
                return;
            }

            // 3. 执行同步操作
            syncCharsetAndCollation(targetConn, tablesToSync, columnsToSync);

            System.out.println("以下 SQL 修改主键字段字符集，需要手工处理");
            for (String temp : PRIMARY_KEY_SQL_SET) {
                System.out.println(temp);
            }

        } catch (SQLException e) {
            System.err.println("数据库操作失败: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 关闭连接
            closeConnection(sourceConn);
            closeConnection(targetConn);
        }
    }

    private static List<TableInfo> findTablesToSync(Connection sourceConn, Connection targetConn) throws SQLException {
        List<TableInfo> tableInfos = new ArrayList<>();

        // 查询上游MySQL表的字符集和排序规则
        String sourceSql =
                "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_COLLATION " +
                        "FROM information_schema.TABLES " +
                        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'";

        // 查询下游TiDB表的字符集和排序规则
        String targetSql =
                "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_COLLATION " +
                        "FROM information_schema.TABLES " +
                        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'";

        // 获取上游表信息
        Map<String, TableInfo> sourceTables = new HashMap<>();
        try (Statement stmt = sourceConn.createStatement();
             ResultSet rs = stmt.executeQuery(sourceSql)) {
            while (rs.next()) {
                String key = rs.getString("TABLE_NAME");
                String collation = rs.getString("TABLE_COLLATION");
                String charset = collation != null ? collation.substring(0, collation.indexOf("_")) : null;

                TableInfo info = new TableInfo(
                        rs.getString("TABLE_SCHEMA"),
                        rs.getString("TABLE_NAME"),
                        charset,
                        collation,
                        null, null // target charset/collation 暂为null
                );
                sourceTables.put(key, info);
            }
        }

        // 获取下游表信息并对比
        try (Statement stmt = targetConn.createStatement();
             ResultSet rs = stmt.executeQuery(targetSql)) {
            while (rs.next()) {
                String key = rs.getString("TABLE_NAME");
                TableInfo sourceInfo = sourceTables.get(key);

                if (sourceInfo != null) {
                    String targetCollation = rs.getString("TABLE_COLLATION");
                    String targetCharset = targetCollation != null ?
                            targetCollation.substring(0, targetCollation.indexOf("_")) : null;

                    // 检查字符集或排序规则是否不一致
                    if (!equalsIgnoreNull(sourceInfo.sourceCollation, targetCollation)) {

                        TableInfo syncInfo = new TableInfo(
                                sourceInfo.tableSchema,
                                sourceInfo.tableName,
                                sourceInfo.sourceCharset,
                                sourceInfo.sourceCollation,
                                targetCharset,
                                targetCollation
                        );
                        tableInfos.add(syncInfo);

                        System.out.println("发现不一致表: " + key);
                        System.out.println("  上游: " + sourceInfo.sourceCharset + "/" + sourceInfo.sourceCollation);
                        System.out.println("  下游: " + targetCharset + "/" + targetCollation);
                    }
                }
            }
        }

        return tableInfos;
    }

    private static List<ColumnInfo> findColumnsToSync(Connection sourceConn, Connection targetConn) throws SQLException {
        List<ColumnInfo> columnInfos = new ArrayList<>();

        // 查询上游MySQL字符类型字段
        String sourceSql =
                "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, " +
                        "CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_DEFAULT, IS_NULLABLE, COLUMN_COMMENT " +
                        "FROM information_schema.COLUMNS " +
                        "WHERE TABLE_SCHEMA = DATABASE() " +
                        "AND DATA_TYPE IN ('varchar', 'char', 'text', 'tinytext', 'mediumtext', 'longtext')";

        // 查询下游TiDB字符类型字段
        String targetSql =
                "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, " +
                        "CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_DEFAULT, IS_NULLABLE, COLUMN_COMMENT " +
                        "FROM information_schema.COLUMNS " +
                        "WHERE TABLE_SCHEMA = DATABASE() " +
                        "AND DATA_TYPE IN ('varchar', 'char', 'text', 'tinytext', 'mediumtext', 'longtext')";

        // 获取上游字段信息
        Map<String, ColumnInfo> sourceColumns = new HashMap<>();
        try (Statement stmt = sourceConn.createStatement();
             ResultSet rs = stmt.executeQuery(sourceSql)) {
            while (rs.next()) {
                String key = rs.getString("TABLE_NAME") + "." + rs.getString("COLUMN_NAME");
                ColumnInfo info = new ColumnInfo(
                        rs.getString("TABLE_SCHEMA"),
                        rs.getString("TABLE_NAME"),
                        rs.getString("COLUMN_NAME"),
                        rs.getString("DATA_TYPE"),
                        rs.getLong("CHARACTER_MAXIMUM_LENGTH"),
                        rs.getString("CHARACTER_SET_NAME"),
                        rs.getString("COLLATION_NAME"),
                        null, null, // target charset/collation 暂为null
                        rs.getString("COLUMN_DEFAULT"),
                        rs.getString("IS_NULLABLE"),
                        rs.getString("COLUMN_COMMENT")
                );
                sourceColumns.put(key, info);
            }
        }

        // 获取下游字段信息并对比
        try (Statement stmt = targetConn.createStatement();
             ResultSet rs = stmt.executeQuery(targetSql)) {
            while (rs.next()) {
                String key = rs.getString("TABLE_NAME") + "." + rs.getString("COLUMN_NAME");
                ColumnInfo sourceInfo = sourceColumns.get(key);

                if (sourceInfo != null) {
                    String targetCharset = rs.getString("CHARACTER_SET_NAME");
                    String targetCollation = rs.getString("COLLATION_NAME");

                    // 检查字符集或排序规则是否不一致
                    if (!equalsIgnoreNull(sourceInfo.sourceCharset, targetCharset) ||
                            !equalsIgnoreNull(sourceInfo.sourceCollation, targetCollation)) {

                        ColumnInfo syncInfo = new ColumnInfo(
                                sourceInfo.tableSchema,
                                sourceInfo.tableName,
                                sourceInfo.columnName,
                                sourceInfo.dataType,
                                sourceInfo.characterMaximumLength,
                                sourceInfo.sourceCharset,
                                sourceInfo.sourceCollation,
                                targetCharset,
                                targetCollation,
                                sourceInfo.columnDefault,
                                sourceInfo.isNullable,
                                sourceInfo.columnComment
                        );
                        columnInfos.add(syncInfo);

                        System.out.println("发现不一致字段: " + key);
                        System.out.println("  上游: " + sourceInfo.sourceCharset + "/" + sourceInfo.sourceCollation);
                        System.out.println("  下游: " + targetCharset + "/" + targetCollation);
                    }
                }
            }
        }

        return columnInfos;
    }

    private static void syncCharsetAndCollation(Connection targetConn,
                                                List<TableInfo> tablesToSync,
                                                List<ColumnInfo> columnsToSync) throws SQLException {
        // 设置手动提交
        targetConn.setAutoCommit(true);

        try {
            // 第一步：同步表字符集和排序规则
            for (TableInfo tableInfo : tablesToSync) {
                String alterTableSql = buildAlterTableSql(tableInfo);
                try (Statement stmt = targetConn.createStatement()) {
                    System.out.println("执行表同步: " + alterTableSql);
                    stmt.execute(alterTableSql);
                } catch (SQLException e) {
                    System.err.println("修改表失败 " + tableInfo.tableName + ": " + e.getMessage());
                    // 根据需求决定是否抛出异常
                }
            }

            // 第二步：同步字段字符集和排序规则（需要处理索引）
            // 按表分组处理字段
            Map<String, List<ColumnInfo>> columnsByTable = new HashMap<>();
            for (ColumnInfo columnInfo : columnsToSync) {
                columnsByTable.computeIfAbsent(columnInfo.tableName, k -> new ArrayList<>()).add(columnInfo);
            }

            for (Map.Entry<String, List<ColumnInfo>> entry : columnsByTable.entrySet()) {
                String tableName = entry.getKey();
                List<ColumnInfo> columns = entry.getValue();

                System.out.println("处理表字段: " + tableName);

                // 1. 收集需要删除的索引
                Map<String, IndexInfo> indexesToDrop = new LinkedHashMap<>(); // 保持顺序
                for (ColumnInfo columnInfo : columns) {
                    List<IndexInfo> indexesOnColumn = findIndexesOnColumn(targetConn, tableName, columnInfo.columnName);
                    for (IndexInfo indexInfo : indexesOnColumn) {
                        // 使用索引名作为键，避免重复
                        if (!indexesToDrop.containsKey(indexInfo.indexName)) {
                            indexesToDrop.put(indexInfo.indexName, indexInfo);
                        }
                    }
                }
                Set<String> primaryKeys = new HashSet<>();

                // 2. 删除索引（主键除外）
                List<IndexInfo> droppedIndexes = new ArrayList<>();
                for (IndexInfo indexInfo : indexesToDrop.values()) {
                    if ("PRIMARY".equals(indexInfo.indexName)) {
                        primaryKeys.addAll(indexInfo.columnNames);
                        System.out.println("  跳过主键索引: " + indexInfo.indexName);
                        continue;
                    }

                    String dropIndexSql = String.format("DROP INDEX `%s` ON `%s`", indexInfo.indexName, tableName);
                    try (Statement stmt = targetConn.createStatement()) {
                        System.out.println("  执行: " + dropIndexSql);
                        stmt.execute(dropIndexSql);
                        droppedIndexes.add(indexInfo);
                    } catch (SQLException e) {
                        System.err.println("  删除索引失败 " + indexInfo.indexName + ": " + e.getMessage());
                        throw e;
                    }
                }

                // 3. 修改字段字符集和排序规则
                for (ColumnInfo columnInfo : columns) {
                    String modifySql = buildModifyColumnSql(columnInfo);
                    if (!primaryKeys.contains(columnInfo.columnName)) {
                        try (Statement stmt = targetConn.createStatement()) {
                            System.out.println("  执行: " + modifySql);
                            stmt.execute(modifySql);
                        } catch (SQLException e) {
                            System.err.println("  修改字段失败 " + columnInfo.columnName + ": " + e.getMessage());
                            throw e;
                        }
                    } else {
                        PRIMARY_KEY_SQL_SET.add(modifySql);
                    }
                }

                // 4. 重新创建索引
                for (IndexInfo indexInfo : droppedIndexes) {
                    String createIndexSql = buildCreateIndexSql(indexInfo);
                    try (Statement stmt = targetConn.createStatement()) {
                        System.out.println("  执行: " + createIndexSql);
                        stmt.execute(createIndexSql);
                    } catch (SQLException e) {
                        System.err.println("  重建索引失败 " + indexInfo.indexName + ": " + e.getMessage());
                        throw e;
                    }
                }
            }

            // 提交事务

            System.out.println("字符集和排序规则同步完成！");

        } catch (SQLException e) {
            System.err.println("同步失败，回滚事务: " + e.getMessage());

            throw e;
        } finally {
            targetConn.setAutoCommit(true);
        }
    }

    private static String buildAlterTableSql(TableInfo tableInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE `").append(tableInfo.tableName).append("` ");

        // 设置表的字符集和排序规则
        if (tableInfo.sourceCharset != null) {
            sb.append("CHARACTER SET ").append(tableInfo.sourceCharset).append(" ");
        }

        if (tableInfo.sourceCollation != null) {
            sb.append("COLLATE ").append(tableInfo.sourceCollation);
        }

        return sb.toString().trim();
    }

    private static List<IndexInfo> findIndexesOnColumn(Connection conn, String tableName, String columnName) throws SQLException {
        List<IndexInfo> indexInfos = new ArrayList<>();

        String sql =
                "SELECT INDEX_NAME, NON_UNIQUE, INDEX_TYPE " +
                        "FROM information_schema.STATISTICS " +
                        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?";

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
                // 获取索引的完整列信息
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
                indexInfo.columnNames.add(rs.getString("COLUMN_NAME"));
                long subPart = rs.getLong("SUB_PART");
                indexInfo.subParts.add(subPart == 0 ? null : subPart);
            }
        }
    }

    private static String buildModifyColumnSql(ColumnInfo columnInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE `").append(columnInfo.tableName).append("` MODIFY COLUMN `")
                .append(columnInfo.columnName).append("` ").append(columnInfo.dataType);

        // 处理长度（VARCHAR/CHAR类型）
        if (columnInfo.characterMaximumLength != null && columnInfo.characterMaximumLength > 0 &&
                ("varchar".equalsIgnoreCase(columnInfo.dataType) || "char".equalsIgnoreCase(columnInfo.dataType))) {
            sb.append("(").append(columnInfo.characterMaximumLength).append(")");
        }

        // 设置字符集和排序规则
        if (columnInfo.sourceCharset != null) {
            sb.append(" CHARACTER SET ").append(columnInfo.sourceCharset);
        }

        if (columnInfo.sourceCollation != null) {
            sb.append(" COLLATE ").append(columnInfo.sourceCollation);
        }

        // 处理其他属性
        if ("NO".equalsIgnoreCase(columnInfo.isNullable)) {
            sb.append(" NOT NULL");
        } else {
            sb.append(" NULL");
        }

        if (columnInfo.columnDefault != null) {
            if ("NULL".equalsIgnoreCase(columnInfo.columnDefault)) {
                sb.append(" DEFAULT NULL");
            } else {
                sb.append(" DEFAULT '").append(columnInfo.columnDefault).append("'");
            }
        }

        if (columnInfo.columnComment != null && !columnInfo.columnComment.isEmpty()) {
            sb.append(" COMMENT '").append(columnInfo.columnComment).append("'");
        }

        return sb.toString();
    }

    private static String buildCreateIndexSql(IndexInfo indexInfo) {
        StringBuilder sb = new StringBuilder();

        if ("PRIMARY".equals(indexInfo.indexName)) {
            sb.append("ALTER TABLE `").append(indexInfo.tableName).append("` ADD PRIMARY KEY (");
        } else if (indexInfo.nonUnique) {
            sb.append("CREATE INDEX `").append(indexInfo.indexName).append("` ON `")
                    .append(indexInfo.tableName).append("` (");
        } else {
            sb.append("CREATE UNIQUE INDEX `").append(indexInfo.indexName).append("` ON `")
                    .append(indexInfo.tableName).append("` (");
        }

        // 构建索引列
        List<String> indexColumns = new ArrayList<>();
        for (int i = 0; i < indexInfo.columnNames.size(); i++) {
            String columnName = indexInfo.columnNames.get(i);
            Long subPart = indexInfo.subParts.get(i);
            if (subPart != null && subPart > 0) {
                indexColumns.add("`" + columnName + "`(" + subPart + ")");
            } else {
                indexColumns.add("`" + columnName + "`");
            }
        }

        sb.append(String.join(", ", indexColumns)).append(")");

        // 添加索引类型（如适用）
        if (!"PRIMARY".equals(indexInfo.indexName) && indexInfo.indexType != null &&
                !"BTREE".equalsIgnoreCase(indexInfo.indexType)) {
            sb.append(" USING ").append(indexInfo.indexType);
        }

        return sb.toString();
    }

    private static boolean equalsIgnoreNull(String str1, String str2) {
        if (str1 == null && str2 == null) return true;
        if (str1 == null || str2 == null) return false;
        return str1.equals(str2);
    }

    private static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                System.err.println("关闭连接失败: " + e.getMessage());
            }
        }
    }
}