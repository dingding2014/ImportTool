package hiveImport;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class MysqlJdbcClient {
	
            private Connection con = null;
			private String url = null;
			private String user = null;
			private String password = null;
			
			public MysqlJdbcClient(String db_ip, String db_port, String username, String passwd, String db_name) {
				url = "jdbc:mysql://"+db_ip+":"+db_port+"/"+db_name;
				user = username;
				password = passwd;
			}
			
			public void init() {
				try {
					Class.forName("com.mysql.jdbc.Driver");
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					con = DriverManager.getConnection(url,user,password);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			public List<String> getAllTables() throws SQLException {
				init();
				Statement statement = null;
				try {
					statement = con.createStatement();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			    String sql = "show tables";
	            ResultSet rs = null;
				try {
					rs = statement.executeQuery(sql);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                List<String> tables = new ArrayList<String>();
			     while(rs.next()) {
			          tables.add( rs.getString(1));
			     }
			     rs.close();
			     statement.close();
			     con.close();
			     return tables;
			}										     			    		
}
