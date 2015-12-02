package hiveImport;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import util.LoadUrl;
 
public class HiveJdbcClient {
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	//private static String driverURL = "jdbc:hive://10.214.208.113:10002/default;auth=noSas1";
	private static String driverURL = "jdbc:hive://"+LoadUrl.hiveURL+"/default;auth=noSas1";
	private  Connection con = null;
	private  Statement stmt = null;
	private  ResultSet res  = null;
	
	public void init()  {
		try{
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		try {
			con = DriverManager.getConnection(driverURL, "", "");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void ConClose() throws SQLException {
		res.close();
		stmt.close();
		con.close();	
	}
	
	public HashMap<String, String> desc(String table_name) throws SQLException {
		HashMap<String, String> fields = new HashMap<String, String>();
		init();
		//regular hive query
		String sql = "desc " + table_name;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		String name;String type;
		while(res.next()) {
			 name = res.getString(1).trim() ;
			 type = res.getString(2).trim();
			 System.out.println(name + " " + type); 
			 fields.put(name, type);
		}
		ConClose();
		return fields;
	}
	
	public boolean checkTableExist(String hive_database, String TableName) throws SQLException  {
		init();
		String sql="use "+hive_database;
		stmt.execute(sql);
		sql = "show tables";
		res = stmt.executeQuery(sql);
		while(res.next()) {
			 String Name =  res.getString(1);
			 if(Name.equals(TableName))  {
				 ConClose();
				 return true;
			 }
		}
		ConClose();
		return false;
	}
}