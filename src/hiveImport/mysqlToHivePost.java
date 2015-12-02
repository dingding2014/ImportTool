package hiveImport;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import hiveImport.HiveJdbcClient;

import net.sf.json.JSONObject;
import util.LoadUrl;


public class mysqlToHivePost {
	//private final static String RequestUrl =  "http://10.214.208.113:8472/api/";
	private final static String RequestUrl =  "http://"+LoadUrl.azkabanURL+"/api/";
	private String dbName;
	private String tableName;
	private String userName;
	public HiveJdbcClient client = new HiveJdbcClient();
	public mysqlToHivePost( String userName,String dbName, String tableName){
		this.dbName = dbName;
		this.tableName = tableName;
		this.userName = userName;
	}
	public  void httpPost() throws SQLException{
		String requestUrl = RequestUrl + userName + "/Schema";
		HashMap<String, String> column = client.desc(dbName,tableName); 
		HashMap<String, String> elem;
		HashMap<String, Object> meta;
		List<HashMap> fields ;
//		String BOUNDARY = "---------7d4a6d158c9"; // ������ݷָ���
//		String namepw = "api/" + userName + "/Schema"; 
		try{
			
			URL url = new URL(requestUrl);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection(); 
			
			conn.setDoInput(true);
			conn.setDoOutput(true);
			conn.setRequestMethod("POST");
			conn.setInstanceFollowRedirects(true);
//			conn.setRequestProperty("Content-Length" , Integer.toString(namepw.length()));
			conn.setRequestProperty("Accept" , "application/json, text/javascript, */*; q=0.01");
			conn.setRequestProperty("Content-Type" , "application/json;charset=UTF-8");
			conn.setRequestProperty("Connection" , "keep-alive");
			//conn.setRequestProperty("Host" , "10.214.208.113:8472");
			conn.setRequestProperty("Host" , LoadUrl.azkabanURL);
			conn.connect();
			
			DataOutputStream out = new DataOutputStream(conn.getOutputStream());
			JSONObject obj = new JSONObject();
			obj.element("namespace", "default");
			obj.element("localName", tableName);
			obj.element("alias",tableName);
			obj.element("user", userName);
			obj.element("category", "RELATION");
			obj.element("type", "Schema");
			meta = new HashMap<String, Object>();
			
			fields = new ArrayList<HashMap>();
			Iterator it = column.entrySet().iterator();
			while(it.hasNext()){
				Map.Entry et = (Entry) it.next();
				elem = new HashMap<String, String>();
				elem.put("comment", "columnInfo");
				elem.put("name", (String) et.getKey());
				elem.put("type", (String) et.getValue());
				fields.add(elem);
			}
			meta.put("fields", fields);
			meta.put("hive_database",dbName);
			obj.element("meta", meta);
			obj.element("detailed", true);
			
			out.writeBytes(obj.toString());
			out.flush();
			
			System.out.println(obj.toString());
			
			System.out.print("Loading ...");
			int status = conn.getResponseCode();
			InputStream input ;
			InputStream error;
			InputStreamReader reader = null;
			if (status == 200) {
				input = conn.getInputStream();
				reader = new InputStreamReader(input, "utf-8");
				BufferedReader br = new BufferedReader(reader);
				
				StringBuffer sb = new StringBuffer();
				String str = null;
				while((str = br.readLine()) != null){
					sb.append(str);
				}
				System.out.println(sb.toString());
				System.out.println("Data IMPORT FINISHED!");
				br.close();
				input.close();
			}
			else {
				error = conn.getErrorStream();
			    // It's likely binary content, use InputStream/OutputStream.
				reader = new InputStreamReader(error,"utf-8");
				BufferedReader br = new BufferedReader(reader);
				
				StringBuffer sb = new StringBuffer();
				String str = null;
				while((str = br.readLine()) != null){
					sb.append(str);
				}
				System.out.println("Table Already Exists!");
				System.out.println(sb.toString());
				br.close();
				error.close();
			}
			
			reader.close();
			out.close();
			conn.disconnect();
			
		} catch(MalformedURLException e){
			System.out.println("URL ERROR!");
			e.printStackTrace();
		} catch(UnsupportedEncodingException e){
			System.out.println("Encoding ERROR!");
			e.printStackTrace();
		} catch(RuntimeException e){
			System.out.println("TimeOut!");
			e.printStackTrace();
		} catch(IOException e){
			System.out.println("Table Already exists!");
			e.printStackTrace();
		}
	}
}