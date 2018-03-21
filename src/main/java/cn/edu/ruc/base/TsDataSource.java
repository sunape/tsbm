package cn.edu.ruc.base;

import cn.edu.ruc.adapter.DBAdapter;
import cn.edu.ruc.cmd.CfgName;

/**
 * ts数据库 数据源配置
 * @author fasape
 *
 */
public class TsDataSource {
	@CfgName(name="DB_DRIVER_CLASS")
	private String driverClass;
	@CfgName(name="DB_IP")
	private String ip;
	@CfgName(name="DB_PORT")
	private String port;
	@CfgName(name="DB_PASSWD")
	private String passwd;
	@CfgName(name="DB_USER")
	private String user;
	public String getDriverClass() {
		return driverClass;
	}
	public void setDriverClass(String driverClass) {
		this.driverClass = driverClass;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}
	public String getPasswd() {
		return passwd;
	}
	public void setPasswd(String passwd) {
		this.passwd = passwd;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	@Override
	public String toString() {
		return "TsDataSource [driverClass=" + driverClass + ", ip=" + ip + ", port=" + port + ", passwd=" + passwd
				+ ", user=" + user + "]";
	}
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		TsDataSource tsds=new TsDataSource();
		tsds.setDriverClass("cn.edu.ruc.adapter.InfluxdbAdapter");
		tsds.setIp("10.77.110.224");
		tsds.setPasswd("123");
		tsds.setUser("user");
		tsds.setPort("8086");
		DBAdapter adapter=(DBAdapter) Class.forName(tsds.getDriverClass()).newInstance();
		adapter.initDataSource(tsds);
		TsQuery query=new TsQuery();
		query.setAggreType(2);
		query.setDeviceName("d_0");
		query.setSensorName("s_0");
		query.setSensorLtValue(54.0);
		query.setSensorGtValue(12.0);
		query.setAggreType(2);
		query.setGroupByUnit(2);
		query.setQueryType(1);
		Object preQuery = adapter.preQuery(query);
		adapter.execQuery(preQuery);
		TsPackage tsPkg=new TsPackage();
		tsPkg.setTimestamp(System.currentTimeMillis());
		tsPkg.setDeviceCode("d2");
		tsPkg.appendValue("s1", 1.1);
		tsPkg.appendValue("s2", 1.2);
		TsWrite tsWrite = new TsWrite();
		tsWrite.append(tsPkg);
		Object preWrite = adapter.preWrite(tsWrite);
		adapter.execWrite(preWrite);
	}
}
