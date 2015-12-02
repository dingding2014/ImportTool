package util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class LoadUrl {
	public  static String azkabanURL ;
	public  static String hiveURL ;
	public void load() throws FileNotFoundException, IOException { 
		 Properties props = new Properties();
		 InputStream in = this.getClass().getResourceAsStream("url.properties");
		 props.load(in);
		 in.close();
		 azkabanURL = props.getProperty("azkaban.url");
		 hiveURL = props.getProperty("hive.url");
	}
}
