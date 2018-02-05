package cn.edu.ruc.biz.db;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import cn.edu.ruc.TSUtils;
import cn.edu.ruc.biz.SystemParam;

/**
 * 业务操作工具类 
 */
public class BizDBUtils2 {
	public static Connection getConnection() {
		Connection conn=null;
	    try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:perform.db");
		} catch (Exception e) {
			e.printStackTrace();
		}
	    return conn;
	}
	public static void closeConnection(Connection conn){
		if(conn!=null){
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 通过sql插入数据
	 * @param sql
	 */
	public static void insertBySqlAndParam(String sql,Object[] params){
		Connection conn=null;
		conn=getConnection();
		QueryRunner runner=new QueryRunner();
		ResultSetHandler<Integer> rsh=new ResultSetHandler<Integer>() {
			@Override
			public Integer handle(ResultSet rs) throws SQLException {
				return 1;
			}
		};
		try {
			runner.insert(conn, sql,rsh,params );
		} catch (Exception e) {
			e.printStackTrace();
		}
		closeConnection(conn);
	}
	/**
	 * 通过sql插入数据
	 * @param sql
	 */
	public static void insertBySqlAndParam(Connection conn,String sql,Object[] params){
		QueryRunner runner=new QueryRunner();
		ResultSetHandler<Integer> rsh=new ResultSetHandler<Integer>() {
			@Override
			public Integer handle(ResultSet rs) throws SQLException {
				return 1;
			}
		};
		try {
			runner.insert(conn, sql,rsh,params );
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 通过表名称，列名称插入数据
	 * @param columns
	 * @param values
	 * @param tableName
	 * @return
	 */
	public static long insertBySqlAndParamAndTable(String[] columns,Object[] values,String tableName){
		Connection conn=null;
		conn=getConnection();
		QueryRunner runner=new QueryRunner();
		ResultSetHandler<Integer> rsh=new ResultSetHandler<Integer>() {
			@Override
			public Integer handle(ResultSet rs) throws SQLException {
				while (rs.next()) {
					return rs.getInt(1);
				}
				return 1;
			}
		};
		long key=-1;
		try {
			StringBuilder sc=new StringBuilder();
			sc.append("insert into ");
			sc.append(tableName);
			sc.append("(");
			int columnSize=columns.length;
			for(int i=0;i<columnSize;i++){
				sc.append(columns[i]);
				if(i<columnSize-1){
					sc.append(",");
				}
			}
			sc.append(") values(");
			for(int i=0;i<columnSize;i++){
				sc.append("?");
				if(i<columnSize-1){
					sc.append(",");
				}
			}
			sc.append(")");
//			System.out.println("sql:"+sc.toString());
			runner.insert(conn, sc.toString(),rsh,values);
			String keySql="select last_insert_rowid() from "+tableName;
			key = (long) runner.query(conn, keySql, rsh);
		} catch (Exception e) {
			e.printStackTrace();
		}
		closeConnection(conn);
		return key;
	}
	/**
	 * 通过表名称，列名称插入数据
	 * @param columns
	 * @param values
	 * @param tableName
	 * @return
	 */
	public static long insertBySqlAndParamAndTable(Connection conn,String[] columns,Object[] values,String tableName){
		QueryRunner runner=new QueryRunner();
		ResultSetHandler<Integer> rsh=new ResultSetHandler<Integer>() {
			@Override
			public Integer handle(ResultSet rs) throws SQLException {
				while (rs.next()) {
					return rs.getInt(1);
				}
				return 1;
			}
		};
		long key=-1;
		try {
			StringBuilder sc=new StringBuilder();
			sc.append("insert into ");
			sc.append(tableName);
			sc.append("(");
			int columnSize=columns.length;
			for(int i=0;i<columnSize;i++){
				sc.append(columns[i]);
				if(i<columnSize-1){
					sc.append(",");
				}
			}
			sc.append(") values(");
			for(int i=0;i<columnSize;i++){
				sc.append("?");
				if(i<columnSize-1){
					sc.append(",");
				}
			}
			sc.append(")");
//			System.out.println("sql:"+sc.toString());
			runner.insert(conn, sc.toString(),rsh,values);
			String keySql="select last_insert_rowid() from "+tableName;
			key = (long) runner.query(conn, keySql, rsh);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return key;
	}
	/**
	 * 通过sql创建表，表存在则不创建，不存在则创建
	 * @param sql
	 * @param tableName
	 */
	public static void createTableBySql(String sql,String tableName){
	    Connection c = null;
	    Statement stmt = null;
		try {
		    Class.forName("org.sqlite.JDBC");
		    c = DriverManager.getConnection("jdbc:sqlite:perform.db");
		    stmt = c.createStatement();
			String countSql="SELECT COUNT(*) num FROM sqlite_master where type='table' and LOWER(name)='"+tableName+"'";
			ResultSet set = stmt.executeQuery(countSql);
			while (set.next()) {
				int num = set.getInt("num");
				if(num==0){
					stmt.executeUpdate(sql);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		    try {
				c.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	public static int selectCountBySql(String countSql) {
		Connection conn=getConnection();
		QueryRunner runner=new QueryRunner();
		ResultSetHandler<Integer> rsh=new ResultSetHandler<Integer>() {
			@Override
			public Integer handle(ResultSet rs) throws SQLException {
				while (rs.next()) {
					return rs.getInt(1);
				}
				return 1;
			}
		};
		int count=0;
		try {
			count=runner.query(conn,countSql, rsh);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return count;
	}
	public static double selectSingleBySql(String avgSql) {
		Connection conn=getConnection();
		QueryRunner runner=new QueryRunner();
		ResultSetHandler<Double> rsh=new ResultSetHandler<Double>() {
			@Override
			public Double handle(ResultSet rs) throws SQLException {
				while (rs.next()) {
					return rs.getDouble(1);
				}
				return 0.0;
			}
		};
		Double avg=0.0;
		try {
			avg=runner.query(conn,avgSql, rsh);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if(avg==null) avg=0.0;
		return avg;
	}
	public static List<Map<String,Object>> selectListBySqlAndParam(String sql,Object... params) {
		//TODO
		Connection conn=getConnection();
		QueryRunner runner=new QueryRunner();
		ResultSetHandler<List<Map<String,Object>>> rsh=new ResultSetHandler<List<Map<String,Object>>>() {
			@Override
			public List<Map<String,Object>> handle(ResultSet rs) throws SQLException {
				List<Map<String,Object>> list=new ArrayList<Map<String,Object>>();
				while (rs.next()) {
					Map<String,Object> map=new HashMap<String, Object>();
					ResultSetMetaData metaData = rs.getMetaData();
					int columns = metaData.getColumnCount();
					for(int i=0;i<columns;i++){
//						String columnName = metaData.getColumnName(i+1);
						String columnName = metaData.getColumnLabel(i+1);
						Object obj=rs.getObject(columnName);
						map.put(columnName, obj);
					}
					list.add(map);
				}
				return list;
			}
		};
		List<Map<String,Object>> list=null;
		try {
//			Object[] params={Constants.LOAD_BATCH_ID};
			list=runner.query(conn,sql, rsh,params);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}
	public static void main(String[] args) {

	}
	public static void initDataBase() {
		String createSql="create table %s(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,batch_id text not null,create_time INTEGER not null,key text not null,value text not null )";
		String tableName="tsbm_param";
		createTableBySql(String.format(createSql, tableName),tableName);
	}
	public static void saveParam() {
		long currentTime=System.currentTimeMillis();
		String batchId=SystemParam.TEST_MODE+"_"+SystemParam.DB_TYPE+"_"+TSUtils.getDateByTime(currentTime);
		String insertSql = "insert into tsbm_param(batch_id,create_time,key,value) values(?,?,?,?)";
		try {
			SystemParam obj = SystemParam.class.newInstance();
			Field[] fields = SystemParam.class.getDeclaredFields();
			for(Field field:fields) {
				String fieldName=field.getName();
				if("IS_INIT".equals(fieldName)){
					continue;
				}
				if("DB_CLASS".equals(fieldName)){
					continue;
				}
				Object value = field.get(obj);
				Object[] objs=new Object[4];
				objs[0]=batchId;
				objs[1]=currentTime;
				objs[2]=field.getName();
				objs[3]=value;
				System.out.println(insertSql);
				insertBySqlAndParam(insertSql,objs );
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}


