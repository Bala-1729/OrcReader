package com.test;

import java.sql.*;

public class PrestoConnection {
    public static void main(String[] args) throws SQLException {
        Connection con = null;
        Statement st = null;
        long count = 0;
        try {
            Class.forName("com.facebook.presto.jdbc.PrestoDriver");
            con = DriverManager.getConnection("jdbc:presto://172.20.61.239:8081/hive/crm_172_20_2_208_db126odb", "test", "");
            st = con.createStatement();

            long startTime = System.nanoTime();
            ResultSet rs = st.executeQuery("select * from crmentityauditlog where auditlogid>415280000000000000 and auditlogid<415289999999999999");
            while (rs.next()){
                long id = rs.getLong("auditlogid");
            }
            long endTime = System.nanoTime();
            rs.close();
            System.out.println("Function execution time : " + (endTime - startTime) / 1000000000.0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if(con!=null) con.close();
            if(st!=null) st.close();
        }
    }
}
