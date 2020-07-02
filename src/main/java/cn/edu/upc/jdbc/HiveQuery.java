package cn.edu.upc.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class HiveQuery {

    private static final Logger logger = LoggerFactory.getLogger(HiveQuery.class);

    public static void main(String[] args) {
        String url = "jdbc:hive2://192.168.3.11:10000/default";
        String username = "hadoop";
        String password = "";
        String driver = "org.apache.hive.jdbc.HiveDriver";

        String sql = "select userid, name, phonenumber from t_hdfs_user";

        ResultSet rs = null;
        PreparedStatement pstmt = null;
        Connection connection = null;

        try{
            try {
                Class.forName(driver);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            connection = DriverManager.getConnection(url, username, password);
            pstmt = connection.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while(rs.next()){
                String userid = rs.getString("userid");
                String name = rs.getString("name");
                String phone = rs.getString("phonenumber");
                logger.info(userid + "\t" + name + "\t" + phone);
            }
        }catch(SQLException ex){
            ex.printStackTrace();
        }finally{
            if(rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(pstmt != null){
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(connection != null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }


    }

}
