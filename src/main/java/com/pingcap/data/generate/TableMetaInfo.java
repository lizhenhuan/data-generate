package com.pingcap.data.generate;

import java.sql.Connection;
import java.util.List;

public class TableMetaInfo {
    private List<ColumnInfo> columnInfoList;
    private String insertSQL;
    private Connection connection;

    public TableMetaInfo(List<ColumnInfo> columnInfoList, String insertSQL, Connection connection) {
        this.columnInfoList = columnInfoList;
        this.insertSQL = insertSQL;
        this.connection = connection;
    }

    public List<ColumnInfo> getColumnInfoList() {
        return columnInfoList;
    }

    public void setColumnInfoList(List<ColumnInfo> columnInfoList) {
        this.columnInfoList = columnInfoList;
    }

    public String getInsertSQL() {
        return insertSQL;
    }

    public void setInsertSQL(String insertSQL) {
        this.insertSQL = insertSQL;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }
}
