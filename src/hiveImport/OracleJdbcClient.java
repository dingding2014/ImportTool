package hiveImport;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class OracleJdbcClient {

	private Connection con = null;
    private PreparedStatement pre = null;// 创建预编译语句对象
    private ResultSet result = null;
    String url = null;
    String user = null;
    String password = null;
    
    public OracleJdbcClient(String db_ip, String db_port, String db_username, String db_password, String db_name) {
    	url = "jdbc:oracle:" + "thin:@"+db_ip+":"+db_port+":"+db_name; //"jdbc:oracle:" + "thin:@127.0.0.1:1521:XE";
    	user =  db_username;
    	password = db_password;
    }
    
    public void init() throws SQLException {
    	try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    	con = DriverManager.getConnection(url, user, password);
    }
    
    public List<String> getAllTables() throws SQLException{
    	init();
    	List<String> tableNames = new ArrayList<String>();
    	try
        {
            String sql = "show tables";
            pre = con.prepareStatement(sql);// 实例化预编译语句
            result = pre.executeQuery();// 执行查询，注意括号中不需要再加参数
            while (result.next())
               tableNames.add(result.getString(1));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }finally {
        	result.close();
        	pre.close();
        	con.close();
        	
        }
    	return tableNames;
    }
    
}
