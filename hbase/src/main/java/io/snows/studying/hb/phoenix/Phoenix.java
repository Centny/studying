package io.snows.studying.hb.phoenix;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.Locale;

import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_BATCH_SIZE;

public class Phoenix {

    public void crud() throws ClassNotFoundException, SQLException {
        GLOBAL_MUTATION_BATCH_SIZE.update(1000);


        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        Connection conn = DriverManager.getConnection("jdbc:phoenix:localhost:2181;UpsertBatchSize=3000");
        PreparedStatement stmt;
        PhoenixConnection pc=(PhoenixConnection)conn;
        int bs=pc.getMutateBatchSize();
        //pc.getMutationState().
        //
        stmt = conn.prepareStatement("drop table if exists test.dms_user");
        stmt.execute();
        stmt.close();
        //
        stmt = conn.prepareStatement("" +
                "create table test.dms_user(\n" +
                "    tid varchar not null primary key,\n" +
                "    username varchar,\n" +
                "    password varchar,\n" +
                "    status integer\n" +
                ")");
        stmt.execute();
        stmt.close();
        //
        //
        stmt = conn.prepareStatement("create index dms_user_username on test.dms_user (username)");
        stmt.execute();
        stmt.close();
        //
        long beg = new Date().getTime();
        int total = 100000;
        stmt = conn.prepareStatement("upsert into test.dms_user values(?,?,?,100)");
        for (int i = 0; i < total; i++) {
            stmt.setString(1,"_"+i);
            stmt.setString(2,"u"+i);
            stmt.setString(3,"p"+i);
            stmt.execute();
            if (i % 3000 == 0) {
                conn.commit();
            }
        }
        conn.commit();
        stmt.close();
        long used = new Date().getTime() - beg;
        System.out.println("used:" + used + ",avg:" + ((float) used / (float) total));
        //
    }


    public static void main(String args[]) {
        try {
            new Phoenix().crud();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
