package cn.edu.ruc.adapter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;

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
	private static String JDBC_POOL_URL="jdbc:TSDB://%s:%s/";
	private static String JDBC_CLASS="com.taosdata.jdbc.TSDBDriver";
	private static String USER="root";
	private static String PASSWD="taosdata";
	private static String URL="http://%s:%s";
	private static String LOGIN_URL="/rest/login/%s/%s";
	private static String SQL_URL="/rest/sql/%s";
	private TsParamConfig tspc;
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
	private Logger logger=LoggerFactory.getLogger(getClass());
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
		this.tspc=tspc;
		JDBC_URL=String.format(JDBC_URL,ds.getIp(),ds.getPort(),DB_NAME,ds.getUser(),ds.getPasswd());
		JDBC_POOL_URL=String.format(JDBC_POOL_URL, ds.getIp(),ds.getPort());
		PASSWD=ds.getPasswd();
		USER=ds.getUser();
		// 初始化参数
		URL=String.format(URL,ds.getIp(),ds.getPort());
		LOGIN_URL = URL +String.format(LOGIN_URL,ds.getUser(),ds.getPasswd());
		SQL_URL = URL +String.format(SQL_URL, DB_NAME);
		initConnectionPool();
//		getDataSource();
//		 创建metric JDBC
		int deviceNum = tspc.getDeviceNum();
		int sensorNum = tspc.getSensorNum();
		StringBuffer fieldAttrs=new StringBuffer();
		fieldAttrs.append("ts timestamp");
		for(int column=0;column<sensorNum;column++) {
			if(column!=0) {
			}
			fieldAttrs.append(",");
			fieldAttrs.append("s_");
			fieldAttrs.append(column);
			fieldAttrs.append(" double");
		}
		String createMetricUrl = String.format("create table %s.%s(%s)"
	    		+ " tags(%s binary(20))"
	    		,DB_NAME,METRIC,fieldAttrs.toString(),DEViCE_TAG);
		Connection conn=null;
		Statement statement=null;
		try {
			//JDBC
			System.out.println(createMetricUrl);
			conn= getConnection();
			statement = conn.createStatement();
			statement.executeUpdate(createMetricUrl);
		} catch (Exception e) {
			e.printStackTrace();
		}
	    closeStatement(statement);
	    closeConnection(conn);
	    System.out.println(createMetricUrl);
		try {
			conn= getConnection();
			statement = conn.createStatement();
				for(int dn=0;dn<deviceNum;dn++) {
						String deviceCode="d_"+dn;
						String createTableSql=String.format("create table %s.%s using sensor tags('%s');"
								,DB_NAME,deviceCode,deviceCode);
						System.out.println(createTableSql);
						statement.executeUpdate(createTableSql);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		closeStatement(statement);
		closeConnection(conn);
		try {
			Thread.sleep(2000L);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Object preWrite(TsWrite tsWrite) {
		LinkedList<TsPackage> pkgs = tsWrite.getPkgs();
		LinkedList<String> lls=new LinkedList<>();
		String formatSql="insert into %s.%s values(%s)";
		for(TsPackage pkg:pkgs) {
//			StringBuffer fields=new StringBuffer();
//			fields.append("ts");
			StringBuffer values=new StringBuffer();
			values.append(pkg.getTimestamp());
			String deviceCode = pkg.getDeviceCode();
			Set<String> sensorCodes = pkg.getSensorCodes();
			for(String sensorCode:sensorCodes) {
//				fields.append(",");
				values.append(",");
//				fields.append(sensorCode);
				values.append(pkg.getValue(sensorCode));
			}
			lls.add(String.format(formatSql, DB_NAME,deviceCode,values.toString()));
		}
		return lls;
	}

	@Override
	public Status execWrite(Object write) {
//		LinkedList<String> lls=(LinkedList<String>) write;
//		for(String sql:lls) {
//			Request request = new Request.Builder()
//					.url(SQL_URL)
//					.post(RequestBody.create(MEDIA_TYPE_TEXT, write.toString()))
//					.header("Authorization", "Bearer "+TOKEN)
//					.build();
//			Status exeOkHttpRequest = exeOkHttpRequest(request);
//		}
//		return exeOkHttpRequest(request);
		Connection conn = getConnection();
		Statement statement =null;
		long costTime=0L;
		try {
			statement = conn.createStatement();
			LinkedList<String> lls=(LinkedList<String>) write;
			long start = System.nanoTime();
			for(String sql:lls) {
				statement.executeUpdate(sql);
//				statement.executeBatch();
			}
//			statement.executeBatch();
			long end = System.nanoTime();
			costTime=end-start;
//			statement.clearBatch();
		} catch (Exception e) {
			e.printStackTrace();
			Status.FAILED(Long.MAX_VALUE);
		}
		closeStatement(statement);
		closeConnection(conn);
		return Status.OK(costTime);
	}

	@Override
	public Object preQuery(TsQuery tsQuery) {
		// TODO Auto-generated method stub
		StringBuffer sc=new StringBuffer();
		sc.append("select ");
		switch (tsQuery.getQueryType()) {
		case 1://简单查询
			sc.append(tsQuery.getSensorName());
			sc.append(" ");
			break;
		case 2://分析查询
			sc.append("");
			if(tsQuery.getAggreType()==1) {
				sc.append("max(");
			}
			if(tsQuery.getAggreType()==2) {
				sc.append("min(");
			}
			if(tsQuery.getAggreType()==3) {
				sc.append("avg(");
			}
			if(tsQuery.getAggreType()==4) {
				sc.append("count(");
			}
			sc.append(tsQuery.getSensorName());
			sc.append(") ");
			break;
		default:
			break;
		}
		sc.append(String.format("from %s.%s where d='%s'",DB_NAME,METRIC,tsQuery.getDeviceName()));
		if(tsQuery.getStartTimestamp()!=null) {
			sc.append("and ");
			sc.append("ts >=");
			sc.append(tsQuery.getStartTimestamp());
			sc.append(" ");
		}
		if(tsQuery.getEndTimestamp()!=null) {
			sc.append("and ");
			sc.append("ts <=");
			sc.append(tsQuery.getEndTimestamp());
			sc.append(" ");
		}
		if(tsQuery.getGroupByUnit()!=null&&tsQuery.getQueryType()==2) {
			sc.append("INTERVAL");
			switch (tsQuery.getGroupByUnit()) {
			case 1:
				sc.append("(1S)");
				break;
			case 2:
				sc.append("(1M)");
				break;
			case 3:
				sc.append("(1H)");
				break;
			case 4:
				sc.append("(1D)");
				break;
			case 5:
				sc.append("(30D)");
				break;
			case 6:
				sc.append("(1Y)");
				break;
			default:
				break;
			}
		}
		return sc.toString();
	}

	@Override
	public Status execQuery(Object query) {
		Connection conn = getConnection();
		Statement statement =null;
		long costTime=0L;
		try {
			statement = conn.createStatement();
			long start = System.nanoTime();
			System.out.println(query.toString());
			ResultSet executeQuery = statement.executeQuery(query.toString());
			if(executeQuery.next()) {
				System.out.println(executeQuery.getObject(0));
			}
			long end = System.nanoTime();
			costTime=end-start;
//			statement.clearBatch();
		} catch (Exception e) {
			e.printStackTrace();
			Status.FAILED(Long.MAX_VALUE);
		}
		closeStatement(statement);
		closeConnection(conn);
		return Status.OK(costTime);
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
//				releaseConnection(conn);
			}
		} catch (Exception e) {
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
	private synchronized  Connection getConnection(){
		Connection connection=null;
		 try {
//			connection = DriverManager.getConnection(JDBC_URL);
//			while(connectionPool.size()==0) {
//				Thread.sleep(1000L);
//			}
//			connection = connectionPool.removeFirst();
			 connection=getDataSource().getConnection();
		} catch (Exception e) {
			e.printStackTrace();
		}
		 return connection;
	}
	   private  DruidDataSource dataSource;
	    private  DruidDataSource getDataSource(){
		    	if(dataSource==null){
		    		synchronized (IotdbAdapter.class) {
		    			if(dataSource==null){
		    				dataSource = new DruidDataSource();  
		    				dataSource.setUsername(USER);  
		    				dataSource.setUrl(JDBC_POOL_URL);  
		    				dataSource.setPassword(PASSWD);  
		    				dataSource.setDriverClassName(JDBC_CLASS);  
		    				dataSource.setInitialSize(10);  
		    				dataSource.setMaxActive(tspc.getWriteClients()+tspc.getReadClients());  
		    				dataSource.setMaxWait(100);  
		    				dataSource.setTestWhileIdle(false);  
		    				dataSource.setTestOnReturn(false);  
		    				dataSource.setTestOnBorrow(false);  
		    				dataSource.setDefaultAutoCommit(false);
		    				dataSource.setDefaultReadOnly(false);
		    			}
					}
		    	}
		    	return dataSource;
	    }
	    private LinkedList<Connection> connectionPool=new LinkedList<>();
	    private synchronized void initConnectionPool() {
	    		if(connectionPool.size()==0) {
	    			synchronized (DB_NAME) {
	    				int max=tspc.getWriteClients()+tspc.getReadClients();
	    				for(int i=0;i<max;i++) {
	    					try {
	    						Connection connection = DriverManager.getConnection(JDBC_URL);
	    						connectionPool.addLast(connection);
	    					} catch (SQLException e) {
	    						e.printStackTrace();
	    					}
	    				}
	    			}
	    		}
	    }
	    private synchronized void releaseConnection(Connection connection) {
	    	    connectionPool.addLast(connection);
	    }
}
