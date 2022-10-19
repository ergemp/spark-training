package org.ergemp.sql.sources;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class JdbcOnHiveTable {
    public static void main(String[] args) {
        String jdbcConn = "jdbc:hive2://localhost:10000/default";

        try {
            String driverName = "org.apache.hive.jdbc.HiveDriver";
            Class.forName(driverName);
            Connection con = DriverManager.getConnection(jdbcConn, "" , "");

            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("show tables");
            while (rs.next()) {
                System.out.println(rs.getString(2));
            }

            /*
            accatalognodes
            accategories
            acorderitems
            acorderitems2
            acorders
            boutiquecounter
            boutiquedetail
            boutiquedetailimpression
            boutiquedetailimpression_json
            boutiquedetail_json
            boutiqueimpressioncounter
            mdboutiqueplan
            newsession
            newsession_json
            ordersummary
            ordersummary_json
            product_content
            productview
            productview_json
            tt
            */
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        finally {
        }
    }
}
