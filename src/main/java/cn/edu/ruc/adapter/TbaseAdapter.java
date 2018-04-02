package cn.edu.ruc.adapter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import cn.edu.ruc.base.TsDataSource;
import cn.edu.ruc.base.TsPackage;
import cn.edu.ruc.base.TsParamConfig;
import cn.edu.ruc.base.TsQuery;
import cn.edu.ruc.base.TsWrite;
import cn.edu.ruc.db.Status;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
/**
 * t-base
 * 使用 restful api
 * okHttp
 * @author fasape
 *
 */
public class TbaseAdapter implements DBAdapter {
	
	private static String JDBC_URL="jdbc:TSDB://%s:%s/%s?user=%s&password=%s";
	private static String JDBC_CLASS="com.taosdata.jdbc.TSDBDriver";
	
	private static String URL="http://%s:%s";
	private static String LOGIN_URL="/rest/login/%s/%s";
	private static String SQL_URL="/rest/sql/%s";
	
	private static  String METRIC="sensor";
	private static String DEViCE_TAG="d";
	private static String SENSOR_TAG="s";
	private static String TOKEN="";
	private static String DB_NAME="test";
	private static final OkHttpClient OK_HTTP_CLIENT = new OkHttpClient().newBuilder()
		       .readTimeout(500000, TimeUnit.MILLISECONDS)
		       .connectTimeout(500000, TimeUnit.MILLISECONDS)
		       .writeTimeout(500000, TimeUnit.MILLISECONDS)
		       .build();
	public static OkHttpClient getOkHttpClient(){
		return OK_HTTP_CLIENT;
	}
	MediaType MEDIA_TYPE_TEXT = MediaType.parse("text/plain");
	public static void main(String[] args) throws Exception {
	}
	@Override
	public void initDataSource(TsDataSource ds,TsParamConfig tspc) {
		try {
			Class.forName(JDBC_CLASS);
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}
		JDBC_URL=String.format(JDBC_URL,ds.getIp(),ds.getPort(),DB_NAME,ds.getUser(),ds.getPasswd());
		// 初始化参数
		URL=String.format(URL,ds.getIp(),ds.getPort());
		LOGIN_URL = URL +String.format(LOGIN_URL,ds.getUser(),ds.getPasswd());
		SQL_URL = URL +String.format(SQL_URL, DB_NAME);
		//初始化token
//	    Request request = null;
//	    		
//	    OkHttpClient client = getOkHttpClient();
//	    try {
//		   request= 	new Request.Builder()
//	            .url(LOGIN_URL)
//	            .post(RequestBody.create(MEDIA_TYPE_TEXT, ""))
//	            .build();
//			Response response = client.newCall(request).execute();
//			String string = response.body().string();
//			JSONObject obj = JSON.parseObject(string);
//			TOKEN=obj.getString("desc");
//			response.close();
//			
//			//JDBC
//			
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	    // 创建database
//	    String createDataBaseUrl = String.format("create database %s",DB_NAME);
//	    try {
//		    	request= new Request.Builder()
//		    			.url(SQL_URL)
//		    			.post(RequestBody.create(MEDIA_TYPE_TEXT, createDataBaseUrl))
//		    			.header("Authorization", "Bearer "+TOKEN)
//		    			.build();
//		    	Response response = client.newCall(request).execute();
//		    	response.code();
//		    	response.close();
//		    	//JDBC
//		    } catch (Exception e) {
//		    	e.printStackTrace();
//	    }
//	    try {
//		    	String createTableSql=String.format("create table %s.sensor (ts timestamp,value double,%s binary(128),%s binary(128))"
//		    			,DB_NAME,DEViCE_TAG,SENSOR_TAG);
//		    	request= new Request.Builder()
//		    			.url(SQL_URL)
//		    			.post(RequestBody.create(MEDIA_TYPE_TEXT, createTableSql))
//		    			.header("Authorization", "Bearer "+TOKEN)
//		    			.build();
//		    	Response response = client.newCall(request).execute();
//		    	System.out.println(response.body().string());
//		    	response.code();
//		    	response.close();
//	    } catch (Exception e) {
//	    	e.printStackTrace();
//	    }
		// 创建metric
//	    String createMetricUrl = String.format("create table %s.%s(ts timestamp,value double)"
//		    		+ " tags(%s binary(20),%s binary(20)))"
//		    		,DB_NAME,METRIC,DEViCE_TAG,SENSOR_TAG);
//	    try {
//		    	request= new Request.Builder()
//		    			.url(SQL_URL)
//		    			.post(RequestBody.create(MEDIA_TYPE_TEXT, createMetricUrl))
//		    			.header("Authorization", "Bearer "+TOKEN)
//		    			.build();
//		    	Response response = client.newCall(request).execute();
//		    	response.code();
//		    	response.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	    int deviceNum = tspc.getDeviceNum();
//	    int sensorNum = tspc.getSensorNum();
//	    for(int dn=0;dn<deviceNum;dn++) {
//	    		String deviceCode="d_"+dn;
//	    		for(int sn=0;sn<sensorNum;sn++) {
//	    			String sensorCode = "s_"+sn;
//	    			// 创建表
//	    			try {
//	    				String createTableSql=String.format("create table %s_%s using sensor tags('%s','%s');"
//	    						,deviceCode,sensorCode,deviceCode,sensorCode);
//	    				request= new Request.Builder()
//	    				.url(SQL_URL)
//	    				.post(RequestBody.create(MEDIA_TYPE_TEXT, createTableSql))
//	    				.header("Authorization", "Bearer "+TOKEN)
//	    				.build();
//	    				Response response = client.newCall(request).execute();
//	    				response.code();
//	    				response.close();
//	    			} catch (Exception e) {
//	    				e.printStackTrace();
//	    			}
//	    		}
//	    }
//	    try {
//    	String createTableSql=String.format("create table %s.sensor (ts timestamp,value double,%s binary(128),%s binary(128))"
//    			,DB_NAME,DEViCE_TAG,SENSOR_TAG);
//    	request= new Request.Builder()
//    			.url(SQL_URL)
//    			.post(RequestBody.create(MEDIA_TYPE_TEXT, createTableSql))
//    			.header("Authorization", "Bearer "+TOKEN)
//    			.build();
//    	Response response = client.newCall(request).execute();
//    	System.out.println(response.body().string());
//    	response.code();
//    	response.close();
//} catch (Exception e) {
//	e.printStackTrace();
//}
//		 创建metric JDBC
		String createMetricUrl = String.format("create table %s.%s(ts timestamp,value double)"
		    		+ " tags(%s binary(20),%s binary(20)))"
		    		,DB_NAME,METRIC,DEViCE_TAG,SENSOR_TAG);
		Connection conn=null;
		Statement statement=null;
		try {
			//JDBC
			conn= (Connection) DriverManager.getConnection(JDBC_URL);
			statement = conn.createStatement();
			statement.execute(createMetricUrl);
		} catch (Exception e) {
			e.printStackTrace();
		}
	    closeStatement(statement);
	    closeConnection(conn);
	    
		int deviceNum = tspc.getDeviceNum();
		int sensorNum = tspc.getSensorNum();
		try {
			conn= (Connection) DriverManager.getConnection(JDBC_URL);
			statement = conn.createStatement();
				for(int dn=0;dn<deviceNum;dn++) {
						String deviceCode="d_"+dn;
						for(int sn=0;sn<sensorNum;sn++) {
							String sensorCode = "s_"+sn;
							// 创建表
								String createTableSql=String.format("create table %s_%s using sensor tags('%s','%s');"
										,deviceCode,sensorCode,deviceCode,sensorCode);
								//JDBC
								statement.addBatch(createTableSql);
				}
			}
			statement.executeBatch();
			statement.clearBatch();
		} catch (Exception e) {
			e.printStackTrace();
		}
		closeStatement(statement);
		closeConnection(conn);
	}

	@Override
	public Object preWrite(TsWrite tsWrite) {
		LinkedList<TsPackage> pkgs = tsWrite.getPkgs();
//		StringBuffer sc=new StringBuffer();
//		String preSql="insert into "+DB_NAME+".sensor";
//		String pattern=",values(%s,%s,'%s','%s')";
//		sc.append(preSql);
//		for(TsPackage pkg:pkgs) {
//			String deviceCode = pkg.getDeviceCode();
//			long timestamp = pkg.getTimestamp();
//			Set<String> sensorCodes = pkg.getSensorCodes();
//			for(String sensorCode:sensorCodes) {
//				Object value = pkg.getValue(sensorCode);
//				sc.append(String.format(pattern,timestamp,value,deviceCode,sensorCode ));
//			}
//		}
//		return sc.toString().replaceFirst(","," ");
		LinkedList<String> lls=new LinkedList<>();
		String formatSql="insert into %s.%s_%s values(%s,%s)";
		for(TsPackage pkg:pkgs) {
			String deviceCode = pkg.getDeviceCode();
			Set<String> sensorCodes = pkg.getSensorCodes();
			for(String sensorCode:sensorCodes) {
				lls.add(String.format(formatSql, DB_NAME,deviceCode,sensorCode,pkg.getTimestamp(),pkg.getValue(sensorCode)));
			}
		}
		return lls;
	}

	@Override
	public Status execWrite(Object write) {
//	    Request request = new Request.Builder()
//	            .url(SQL_URL)
//	            .post(RequestBody.create(MEDIA_TYPE_TEXT, write.toString()))
//	            .header("Authorization", "Bearer "+TOKEN)
//	            .build();
//	    System.out.println(write);
//		return exeOkHttpRequest(request);
		Connection conn = getConnection();
		Statement statement =null;
		long costTime=0L;
		try {
			statement = conn.createStatement();
			LinkedList<String> lls=(LinkedList<String>) write;
			for(String sql:lls) {
				statement.addBatch(sql);
			}
			long start = System.nanoTime();
			statement.executeBatch();
			long end = System.nanoTime();
			costTime=end-start;
			statement.clearBatch();
		} catch (Exception e) {
			e.printStackTrace();
		}
		closeStatement(statement);
		closeConnection(conn);
		return Status.OK(costTime);
	}

	@Override
	public Object preQuery(TsQuery tsQuery) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status execQuery(Object query) {
		// TODO Auto-generated method stub
		return null;
	}
	private Status exeOkHttpRequest(Request request) {
		long costTime = 0L;
	    Response response;
	    OkHttpClient client = getOkHttpClient();
		try {
			long startTime1=System.nanoTime();
			response = client.newCall(request).execute();
			int code = response.code();
			response.close();
			long endTime1=System.nanoTime();
			costTime=endTime1-startTime1;
		} catch (Exception e) {
			e.printStackTrace();
			return Status.FAILED(0L);
		}
		return Status.OK(costTime);
	}
	private void closeConnection(Connection conn){
		try {
			if(conn!=null){
				conn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	private void closeStatement(Statement statement){
		try {
			if(statement!=null){
				statement.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	private  Connection getConnection(){
		Connection connection=null;
		 try {
			connection = DriverManager.getConnection(JDBC_URL);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		 return connection;
	}
}
