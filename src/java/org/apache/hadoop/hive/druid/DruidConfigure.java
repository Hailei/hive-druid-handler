package org.apache.hadoop.hive.druid;

import java.util.Properties;

public class DruidConfigure {
	
	
	private Properties properties;
	
	
	public DruidConfigure(Properties properties){
		
		this.properties = properties;
	}
	
	
	public String get(String name){
		
		return properties.getProperty(name);

	}
	
	public String get(String name,String defaultValue){
		
		String value = get(name);
		
		return value == null ? defaultValue:value;
	}
	
	public long getLong(String name,long defaultValue ){
		
		String value = get(name);
		if(value != null){
			
			return Long.parseLong(value);
		}else{
			return defaultValue;
		}
			
	}
	
	
	public int getInt(String name,int defaultValue){
		
		String value = get(name);
		
		if(value != null){
			return Integer.parseInt(value);
		}else{
			return defaultValue;
		}
		
	}

}
