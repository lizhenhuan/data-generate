package com.pingcap.data.generate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Client {
    public static void main(String args[]) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            //Connection con = DriverManager.getConnection( "jdbc:mysql://127.0.0.1:4000/test?useServerPrepStmts=true&rewriteBatchedStatements=true&allowMultiQueries=true", "root", "");
            Connection con = DriverManager.getConnection( "jdbc:mysql://127.0.0.1:4000/test?useCursorFetch=true&defaultFetchSize=1000&useServerPrepStmts=true&rewriteBatchedStatements=true&allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8", "root", "");            //PreparedStatement stmt1 = con.prepareStatement("select 1; select 1;");
            //con.createStatement().execute("set max_execution_time = 100");
            PreparedStatement stmt1 = con.prepareStatement("SELECT id, name,  (select @@collation_connection) collation_connection FROM jan force index(idx_name) where name like '广%'");
            ResultSet rs = stmt1.executeQuery();
            while (rs.next()) {
                int id  = rs.getInt("id");
                String name = rs.getString("name");
                String collation_connection = rs.getString("collation_connection");
    
                // 输出数据
                System.out.print("ID: " + id);
                System.out.print(", 名称: " + name);
                System.out.print(", collation_connection: " + collation_connection);
                System.out.print("\n");
            }
            rs.close();
            stmt1.close();
            con.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
} 