package com.pingcap.data.generate;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.util.Random;
import java.util.UUID;

public class ColumnInfo {
    static int UUID_LENGTH = 36;
    private String columnName;
    private String dataType;
    private String columnKey;
    private String extra;
    private long maxLength;
    private String isNullable;
    private int handleType;
    private int handleMaxLength;

    public int getHandleType() {
        return handleType;
    }

    public void setHandleType(int handleType) {
        this.handleType = handleType;
    }

    public int getHandleMaxLength() {
        return handleMaxLength;
    }

    public void setHandleMaxLength(int handleMaxLength) {
        this.handleMaxLength = handleMaxLength;
    }

    public ColumnInfo(String columnName, String dataType, String columnKey, String extra, long maxLength, String isNullable) throws Exception {
        this.columnName = columnName;
        this.dataType = dataType;
        this.columnKey = columnKey;
        this.extra = extra;
        this.maxLength = maxLength;
        this.isNullable = isNullable;
        if (getDataType().contains("tinyint") || getDataType().contains("smallint")) {
            this.handleType = 1;
        } else if (getDataType().contains("int") || getDataType().contains("decimal")) {
            this.handleType = 2;
        } else if (getDataType().contains("char") || getDataType().contains("text")) {
            this.handleType = 3;
            this.handleMaxLength = UUID_LENGTH > (int)maxLength ? (int)maxLength : UUID_LENGTH;
        } else if (getDataType().contains("date")) {
            this.handleType = 4;
        } else if (getDataType().contains("double")) {
            this.handleType = 5;
        } else {
            throw new Exception("unknow type " + getDataType());
        }
    }


    static long millSecond = System.currentTimeMillis();

    public void setPreparedStatement(PreparedStatement ps, int index, Random random) throws Exception {
        switch (handleType) {
            case 1:
                ps.setInt(index, random.nextInt(100));
                break;
            case 2:
                ps.setInt(index, random.nextInt(100000000));
                break;
            case 3:
                ps.setString(index, UUID.randomUUID().toString().substring(0, handleMaxLength - 1));
                break;
            case 4:
                ps.setDate(index, new Date(millSecond - random.nextInt(100000000)));
                break;
            case 5:
                ps.setDouble(index, random.nextDouble());
                break;
        }
    }

    public String generateRelationColumnSQL(String sourceColumn, String relationTable, String relationColumn) throws Exception {
        Random random = new Random();
        if (sourceColumn.equals(columnName)) {
            return relationTable + "." + relationColumn + " as " + columnName + " ,";
        }
        switch (handleType) {
            case 1:
                return random.nextInt(100) + " as " + columnName + " ,";
            case 2:
                return random.nextInt(1000000) + " as " + columnName + " ,";
            case 3:
                return " left(uuid()," + maxLength +" ) as " + columnName + " ,";
            case 4:
                return " '2022-09-10 23:25:34' as " + columnName + ",";
        }
        throw new Exception("Unknow handle type");
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getColumnKey() {
        return columnKey;
    }

    public void setColumnKey(String columnKey) {
        this.columnKey = columnKey;
    }

    public long getMaxLength() {
        return maxLength;
    }

    public void setMaxLength(long maxLength) {
        this.maxLength = maxLength;
    }

    public String getIsNullable() {
        return isNullable;
    }

    public void setIsNullable(String isNullable) {
        this.isNullable = isNullable;
    }
}
