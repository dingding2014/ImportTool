package hiveImport;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import util.LoadUrl;

public class MysqlToHive {

	public List<String> cmdString(String option,  String db_type, String address, String port, String dbbusername, String dbpassword,
			String ext_database, String ext_tablename, String hive_database, String hive_tablename, String mapNum, String colums, String whereconditions,
			String jobName, String checkColumn, String lastValue) {
		List<String> cmdList = new ArrayList<String>();
		if(option.equals("3")) {
			cmdList.add("sqoop");
			cmdList.add("job");
			cmdList.add("--create");
			cmdList.add(jobName);
			cmdList.add("--");
			cmdList.add("import");
		}	    
		else {
			cmdList.add("sqoop");
			cmdList.add("import");		
		}	
		cmdList.add("--connect");
		if(db_type.equalsIgnoreCase("mysql")) {
			cmdList.add("jdbc:mysql://"+address+":"+port+"/"+ext_database);
		}
		else if(db_type.equalsIgnoreCase("oracle")) {
			cmdList.add("jdbc:oracle:thin:@"+address+":"+port+":"+ext_database);
		}
		cmdList.add("--username");
		cmdList.add(dbbusername);
		cmdList.add("--password");
		cmdList.add(dbpassword);
		cmdList.add("--table");
		cmdList.add(ext_tablename);
		cmdList.add("--hive-import");
		cmdList.add("--hive-database");
		cmdList.add(hive_database);
		if(!hive_tablename.equals("null")) {
			cmdList.add("--hive-table");
		    cmdList.add(hive_tablename);
		}
		cmdList.add("--hive-drop-import-delims");
		if (option.equals("0")) {
			cmdList.add("--hive-overwrite");
		}
		cmdList.add("-m");
		if(!mapNum.equals( "null"))  {
			cmdList.add(mapNum);
		}
		else 	{
			cmdList.add("1");
		}
		if (!colums.equals( "null")) {
			cmdList.add("--columns");
			cmdList.add(colums);
		}
		if (!whereconditions.equals( "null")) {
			cmdList.add("--where");
			cmdList.add( whereconditions );
		}
		if(option.equals("3")) {
			cmdList.add("--incremental");
			cmdList.add("lastmodified");
			cmdList.add("--check-column");
			cmdList.add(checkColumn);
			cmdList.add("--last-value");
			cmdList.add(lastValue);
		}
		return cmdList;
	}

	public boolean checkTableExist(String hive_database, String TableName) throws IOException {
		HiveJdbcClient hiveJdbcClient = new HiveJdbcClient();
		boolean exist = false;
		try {
			 exist = hiveJdbcClient.checkTableExist(hive_database,TableName);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return exist;
	}
	
	public Map<String,String> getTableInfo(String jobName) throws IOException {
		Process process = Runtime.getRuntime().exec("sqoop job --show "+jobName);
		InputStream inputStream = process.getInputStream();
		BufferedReader inputbr = new BufferedReader(new InputStreamReader(inputStream));
		String line = null;
		String hiveDatabase = null;
		String hiveTable = null;
		Map<String,String> map = new HashMap<String,String>();
		try {
			while ((line = inputbr.readLine()) != null) {
				int pos = line.indexOf("hive.database.name");
				if(pos>=0) {
					hiveDatabase = line.substring(pos+21);
				}
				pos = line.indexOf("hive.table.name");
				if(pos>=0) {
					hiveTable = line.substring(pos+18);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		map.put("hiveDatabase",hiveDatabase);
		map.put("hiveTable",hiveTable);
		return map;
	}
	
	public static void main(String args[]) throws InterruptedException, FileNotFoundException, IOException, SQLException {
		//load url.properties
		LoadUrl loadURL = new LoadUrl();
	    loadURL.load();
		MysqlToHive mysqlToHive = new MysqlToHive();
	    List<String> cmdList = new ArrayList<String>();
		String username = null;
		String hive_tablename = null;
		String jobName = null;
		String db_type = null;
		String address = null;
		String port = null;
		String dbusername = null;
		String dbpassword = null;
		String ext_database = null;
		String hive_database = null;
		String option = args[0]; // 0 ordinary import,1 where incremental import,2 import all tables of a database,3 create
									// import job,4 execute import job, 5 sqoop job list, 6 show certain sqoop job in details,7 delete a sqoop job
		boolean sendInfo = false;
		if(option.equals("2")) {   //import all tables of a database
			username = args[1]; // "admin";
			db_type = args[2];
			address = args[3];
			port = args[4];
			dbusername = args[5];
			dbpassword = args[6];
		    ext_database = args[7];
		    hive_database=args[8];
		    if(hive_database.equals("null")) {
		    	hive_database="default";
		    }
			String mapNum = args[9];
			cmdList.add("sqoop");
			cmdList.add("import-all-tables");
			cmdList.add("--connect");
			if(db_type.equalsIgnoreCase("mysql")) {
				cmdList.add("jdbc:mysql://"+address+":"+port+"/"+ext_database);
			}
			else if(db_type.equalsIgnoreCase("oracle")) {
				cmdList.add("jdbc:oracle:thin:@"+address+":"+port+":"+ext_database);
			}
			cmdList.add("--username");
			cmdList.add(dbusername);
			cmdList.add("--password");
			cmdList.add(dbpassword);
			cmdList.add("--hive-import");
			cmdList.add("--hive-database");
			cmdList.add(hive_database);	
			cmdList.add("--hive-drop-import-delims");
			cmdList.add("--hive-overwrite");
			cmdList.add("-m");
			if(!mapNum.equals( "null"))  {
				cmdList.add(mapNum);
			}
			else 	{
				cmdList.add("1");
			}
		}else if (option.equals("4")) {
			username = args[1];
			jobName = args[2];
			cmdList.add("sqoop");
			cmdList.add("job");
			cmdList.add("--exec");
			cmdList.add(jobName);
		} else if(option.equals("5")) {
			cmdList.add("sqoop");
			cmdList.add("job");
			cmdList.add("--list");
		}else if(option.equals("6")) {
			jobName = args[1];
			cmdList.add("sqoop");
			cmdList.add("job");
			cmdList.add("--show");
			cmdList.add(jobName);
		}else if(option.equals("7")) {
			jobName = args[1];
			cmdList.add("sqoop");
			cmdList.add("job");
			cmdList.add("--delete");
			cmdList.add(jobName);
		}
		else {
			username = args[1]; // "admin";
			db_type = args[2];
			address = args[3];
			port = args[4];
			dbusername = args[5];
			dbpassword = args[6];
			ext_database = args[7];
			String ext_tablename = args[8];
			hive_database=args[9];
			if(hive_database.equals("null")) {
		    	hive_database="default";
		    }
			hive_tablename = args[10].toLowerCase();// "workflowTest";
			if(hive_tablename.equals("null"))
				 hive_tablename = ext_tablename;
			String mapNum = args[11];
			String colums = "null";
			String whereconditions = "null";
			String checkColumn = "null";
			String lastValue = "null";
			if (args.length > 12) {
				colums = args[12];
			}
			if (args.length > 13) {
				whereconditions = args[13];
			}
			if (args.length > 14) {
				jobName = args[14];
			}
			if (args.length > 15) {
				checkColumn = args[15];
			}
			if (args.length > 16) {
				lastValue = args[16];
			}
		    cmdList = mysqlToHive.cmdString(option,  db_type, address, port, dbusername, dbpassword, ext_database,
						ext_tablename, hive_database,hive_tablename, mapNum, colums, whereconditions, jobName, checkColumn,lastValue);	
		}
		if(option.equals("0")||option.equals("2")) 
			sendInfo = true;
		else if(option.equals("1")) {
			 if(!mysqlToHive.checkTableExist(hive_database,hive_tablename)) {
				 sendInfo = true;
			 }
		}
		else if(option.equals("4")) {
			Map<String,String> map= mysqlToHive.getTableInfo(jobName);
			hive_database = map.get("hiveDatabase");
			hive_tablename = map.get("hiveTable");
			if(!mysqlToHive.checkTableExist(hive_database, hive_tablename)) {
				 sendInfo = true;
			 }
		}
		System.out.println("You are running cmd: ");
		for(String it:cmdList) {
			System.out.print(it+" ");
		}
		System.out.print("\n");
		
		Process process = null;
		
		try {		
			String[] cmdString=new String[cmdList.size()];
			cmdList.toArray(cmdString);
			ProcessBuilder builder = new ProcessBuilder(cmdString);
			builder.redirectErrorStream(true);
			process = builder.start();
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));    
		    String readLine=null;
		    while (null != (readLine = br.readLine()))  
		    {    
		       System.out.println(readLine);     
		    }  
		    process.waitFor(); 
			if(sendInfo) {
				List<String> tableNames = new ArrayList<String>();
				if(option.equals("2")) {
					if(db_type.equals("mysql")) {
						MysqlJdbcClient mysqlJdbc = new MysqlJdbcClient(address,port,dbusername,dbpassword,ext_database);
						tableNames = mysqlJdbc.getAllTables();
					}
					else if(db_type.equals("oracle")) {
						OracleJdbcClient oracleJdbc = new OracleJdbcClient(address,port,dbusername,dbpassword,ext_database);
						tableNames = oracleJdbc.getAllTables();
					}
				}
				else tableNames.add(hive_tablename);
				for(String tableName:tableNames) {
					mysqlToHivePost post = new mysqlToHivePost(username, hive_database, tableName);
					try {
				          post.httpPost();
				   } catch (SQLException e) {
					     System.out.println("IMPORT TO HIVE EXISTS ERROR!");
					     e.printStackTrace();
				   }
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}