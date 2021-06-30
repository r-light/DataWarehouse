package HbaseToMysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlTest {
    static Connection conn;
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
        conn = DriverManager.getConnection("jdbc:mysql://172.31.17.26:3306/lab2" +
                        "?useUnicode=true&autoReconnect=true&failOverReadOnly=false",
                "root",
                "cluster");

        String sql = "INSERT into UserInfo(Uid, Degree, InterestName, Publish, View, Comment, LastTime) VALUES(?,?,?,?,?,?,?)";
        PreparedStatement ps = conn.prepareStatement(sql);
        String ID = "123";
        int EDU = 1;
        String INT = "我";
        String LAST_TIME = "我";
        int PUBLISH = 1;
        int VIEW = 1;
        int COMMENT = 1;
        ps.setString(1, ID);
        ps.setInt(2, EDU);
        ps.setString(3, INT);
        ps.setInt(4, PUBLISH);
        ps.setInt(5, VIEW);
        ps.setInt(6, COMMENT);
        ps.setString(7, LAST_TIME);
        ps.executeUpdate();
    }
}
