package cn.edu.ruc.db.opentsdb;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import com.alibaba.fastjson.JSON;

import cn.edu.ruc.biz.Core;
import cn.edu.ruc.biz.SystemParam;
import cn.edu.ruc.db.DBBase;
import cn.edu.ruc.db.Status;
import cn.edu.ruc.db.TsPoint;
import cn.edu.ruc.db.opentsdb.entity.PointPut;
import cn.edu.ruc.db.opentsdb.entity.PointPutTag;
import cn.edu.ruc.enums.AggreType;

/**
 * Hello world!
 *
 */
public class Opentsdb extends DBBase
{
	private static String URL="http://%s:%s";
	private static String PUT_URL="/api/put";
	private static String QUERY_URL="/api/query";
	public static  String METRIC="wind.val.perform";//time.series.perform
	private static String DEViCE_TAG="d";
	private static String SENSOR_TAG="s";
    public static void main( String[] args ) throws Exception
    {  
    		Core.main(args);
    }
    @Override
    public void init() {  // 形成url
    	super.init();  
	    	URL = String.format(URL,SystemParam.DB_IP,SystemParam.DB_PORT);
	    	PUT_URL=URL+PUT_URL;//"http://192.168.31.128:4242/api/put";
	    	QUERY_URL=URL+QUERY_URL;
	    	HttpPoolManager.init();
    } 
    
	@Override
	public String getDBUrl() {
		/*
		 * 获取数据库服务器ip+端口号 唯一标识这个数据库服务器
		 */
		return URL;
	}


    //连接数据库
	private HttpClient getHttpClient(){
		return HttpPoolManager.getHttpClient();
		
	}
	//insert 部分将转换后的json格式数据插入数据库
	private Status insertByHttpClient(String data){
		HttpClient hc = getHttpClient();
		HttpPost post=new HttpPost(PUT_URL);
		HttpResponse response=null;
		long costTime=0L;
		try {
			HttpEntity entity = new StringEntity(data);
			post.setEntity(entity);
			long startTime = System.nanoTime();
			response = hc.execute(post);
			System.out.println(EntityUtils.toString(response.getEntity()));
			long endTime = System.nanoTime();
			costTime=endTime-startTime; 
		} catch (Exception e) {
			e.printStackTrace();
			return Status.FAILED(-1);
		}
		int statusCode=response.getStatusLine().getStatusCode();
		if (statusCode>=200&&statusCode<300) {
			return Status.OK(costTime);
		}else{
			return Status.FAILED(costTime);
		}
	}
	
	
	@Override
	public Status insertMulti(List<TsPoint> points) {
		ArrayList<PointPut> list2 = new ArrayList<PointPut>();
		if(points!=null){
			 for(TsPoint point :points) {  //points赋值
				 	if(point==null){
				 		break;
				 	}
			        PointPut pointPut=new PointPut();
			        pointPut.setTimestamp(""+point.getTimestamp());
		 	        pointPut.setValue(point.getValue());
			        PointPutTag tag=new PointPutTag(point);//变量host和变量dc所对应的值，复制
			        pointPut.setTags(tag);
			        list2.add(pointPut); //中间的			        
				}
		}
		String json =JSON.toJSONString(list2);//调用这个工具JSON.toJSONString将输入的数据转换为json格式
		return insertByHttpClient(json);	    
	}
	public Status selectByDeviceAndSensor(String device,String sensor, Date startTime,
			Date endTime) {
	    Map<String,Object> map = new  HashMap<String,Object>();
		map.put("start",startTime.getTime());
		map.put("end", endTime.getTime());
		Map<String,Object> subQuery = new HashMap<String ,Object>();
		subQuery.put("aggregator", "sum");
		subQuery.put("metric", "METRIC");
		Map<String,Object> subTag = new HashMap<String ,Object>();
		subTag.put(DEViCE_TAG,device);
		subTag.put(SENSOR_TAG,sensor);
		subQuery.put("tags", subTag);
		List<Map<String,Object>> list = new ArrayList<>();
		list.add(subQuery);
		map.put("queries",list);
		String json=JSON.toJSONString(map);		
		return queriesByHttpClient(json);
	}
	@Override
	public Status selectByDeviceAndSensor(TsPoint point, Date startTime,
			Date endTime) {
		return selectByDeviceAndSensor(point.getDeviceCode(), point.getSensorCode(), startTime, endTime);
	}
	private Status queriesByHttpClient(String data){
		HttpClient hc = getHttpClient();
		HttpPost post=new HttpPost(QUERY_URL);
		HttpResponse response=null;
		long costTime=0L;
		try {
			HttpEntity entity = new StringEntity(data);
			post.setEntity(entity);
			long startTime = System.nanoTime();
			response = hc.execute(post);
			long endTime = System.nanoTime();
			costTime=endTime-startTime;
		} catch (Exception e) {
			e.printStackTrace();
			return Status.FAILED(-1);
		}
		int statusCode=response.getStatusLine().getStatusCode();
		if (statusCode>=200&&statusCode<300) {
			return Status.OK(costTime);
		}else{
			return Status.FAILED(costTime);
		}
	}
	@Override
	public Status selectByDeviceAndSensor(TsPoint point, Double max,
			Double min, Date startTime, Date endTime) {

		//FIXME 当前版本2.3，目前是没有看到相关资料能通过httpAPI来实现此功能的。
		
		return null;
	}

	@Override
	public Status selectByDevice(TsPoint point, Date startTime, Date endTime) {
		return selectByDeviceAndSensor(point.getDeviceCode(), "*", startTime, endTime);
	}

	@Override
	public Status selectDayMaxByDevice(TsPoint point, Date startTime,
			Date endTime) {
		return selectTimeAggreByDeviceAndSensor(AggreType.TIME_HOUR,24,AggreType.MAX, point.getDeviceCode(), "*", startTime, endTime);
	}

	@Override
	public Status selectDayMinByDevice(TsPoint point, Date startTime,
			Date endTime) {
		return selectTimeAggreByDeviceAndSensor(AggreType.TIME_HOUR,24,AggreType.MIN, point.getDeviceCode(), "*", startTime, endTime);
	}

	@Override
	public Status selectDayAvgByDevice(TsPoint point, Date startTime,
			Date endTime) {
			return selectTimeAggreByDeviceAndSensor(AggreType.TIME_HOUR,24,AggreType.AVG, point.getDeviceCode(), "*", startTime, endTime);

	}

	@Override
	public Status selectHourMaxByDevice(TsPoint point, Date startTime,
			Date endTime) {
		return selectTimeAggreByDeviceAndSensor(AggreType.TIME_HOUR,1,AggreType.MAX, point.getDeviceCode(), "*", startTime, endTime);
	}

	@Override
	public Status selectHourMinByDevice(TsPoint point, Date startTime,
			Date endTime) {
			return selectTimeAggreByDeviceAndSensor(AggreType.TIME_HOUR,1,AggreType.MIN, point.getDeviceCode(), "*", startTime, endTime);

	}

	@Override
	public Status selectHourAvgByDevice(TsPoint point, Date startTime,
			Date endTime) {
			return selectTimeAggreByDeviceAndSensor(AggreType.TIME_HOUR,1,AggreType.AVG, point.getDeviceCode(), "*", startTime, endTime);
	}

	@Override
	public Status selectMinuteMaxByDeviceAndSensor(TsPoint point,
			Date startTime, Date endTime) {
		return selectTimeAggreByDeviceAndSensor(AggreType.TIME_MINUTE,1,AggreType.MAX, point.getDeviceCode(), point.getSensorCode(), startTime, endTime);
	}

	@Override
	public Status selectMinuteMinByDeviceAndSensor(TsPoint point,
			Date startTime, Date endTime) {
			return selectTimeAggreByDeviceAndSensor(AggreType.TIME_MINUTE,1,AggreType.MIN, point.getDeviceCode(), point.getSensorCode(), startTime, endTime);
	}

	@Override
	public Status selectMinuteAvgByDeviceAndSensor(TsPoint point,
			Date startTime, Date endTime) {
			return selectTimeAggreByDeviceAndSensor(AggreType.TIME_MINUTE,1,AggreType.AVG, point.getDeviceCode(), point.getSensorCode(), startTime, endTime);
	}

	private Status selectTimeAggreByDeviceAndSensor(String timeUnit,int interval,String aggerType,String device,String sensor,
			Date startTime, Date endTime) {
		Map<String,Object> map = new  HashMap<String,Object>();
		map.put("start",startTime.getTime());
		map.put("end", endTime.getTime());
		Map<String,Object> subQuery = new HashMap<String ,Object>();
		subQuery.put("aggregator", "avg");
		subQuery.put("metric", METRIC);
		subQuery.put("downsample", String.format("%s%s-%s", interval,timeUnit,aggerType));
		Map<String,Object> subTag = new HashMap<String ,Object>();
		subTag.put(DEViCE_TAG,device);
		subTag.put(SENSOR_TAG,sensor);
		subQuery.put("tags", subTag);	
		List<Map<String,Object>> list = new ArrayList<>(); 
		list.add(subQuery);
		map.put("queries",list);
		String json=JSON.toJSONString(map);
		return queriesByHttpClient(json);
	}

	@Override
	public Status updatePoints(List<TsPoint> points) {
		// TODO Auto-generated method stub
		/*
		 * 更新数据
		 * 更新数据可以通过重新赋值来操作。即在要更新的时间，时间戳重新输入相应的数据。通过insert来操作
		 * 注意：更改配置文件的默认设置tsd.storage.fix_duplicates (2.1) 将原来的false 改为true
		 * 默认并没有配置文件，需要在 opentsdb/build目录下的opentsdb.conf（如果没有则创建），在里面添加sd.storage.fix_duplicates=true
		 */
        ArrayList<PointPut> list2 = new ArrayList<PointPut>();
		if(points!=null){
			 for(TsPoint point :points) {  //points赋值
			        PointPut pointPut=new PointPut();
			        pointPut.setTimestamp(""+point.getTimestamp());
		 	        pointPut.setValue(point.getValue());
			        PointPutTag tag=new PointPutTag(point);
			        pointPut.setTags(tag);
			        list2.add(pointPut); 		        
				}
		}
		String json =JSON.toJSONString(list2);
		return insertByHttpClient(json);	
	
	}

	@Override
	public Status deletePoints(Date date) {
		//这里删除好像是要改配置文件的，默认好像有什么情况是不能删除的。
		/*
		 * 删除某个时间点之前的所有数据
		 * 参考
		 * 
		 * 
		 *1、 http://opentsdb.net/docs/build/html/api_http/dropcaches.html  这个是删除内存中缓存的数据
		 * 
		 * 2、通过查询来删除数据http://opentsdb.net/docs/build/html/api_http/query/index.html#
		 * 上面连接中的第二段话，与下面的配置文件相联系
		 * 		一个相关的配置文件http://opentsdb.net/docs/build/html/user_guide/configuration.html
		 * 		中的tsd.http.query.allow_delete	Boolean	Optional	
		 * 		Whether or not to allow deleting data points from storage during query time.	False
		 * 		须将默认的false改为true
		 *   查询http://opentsdb.net/docs/build/html/api_http/query/index.html#verbs
		 *   有verb动词，delete，是不是可以和其他的一样用？只是将连接http的get、post改为delete？
		 *3、http://opentsdb.net/docs/build/html/api_http/index.html#verbs中的delete,有一个删除注释的例子
		 * 删除其他数据该如何删除？
		 *在Telnet Style API中有 dropcaches
		 *
		 *
		 *结论： 删除指定时间范围内的数据是不能通过Http来操作的。
		 */
		return null;
	}
	/**
	 * 数据生成器
	 */
	@Override
	public long generateData() {
		super.generateData();
		String path = getDataPath();
		File file = new File(path + "/" + "opentsdb_" + System.nanoTime());
		if (file.exists()) {
			file.delete();
		}
		StringBuilder sc = new StringBuilder();
		long startTime = System.nanoTime();
		long count = 0;
		try {
			FileWriter fw;
			fw = new FileWriter(file, true);
			fw.write(sc.toString());
			fw.close();
			int sumTimes = getSumTimes();// 根据总生成条数设置
			for (int i = 0; i < sumTimes; i++) {
				fw = new FileWriter(file, true);
				sc.setLength(0);
				List<TsPoint> tsFiles = Core.generateLoadData(sumTimes, i + 1);
				for (TsPoint point : tsFiles) {
					sc.append(METRIC);//
					sc.append(" ");
					sc.append(DEViCE_TAG);
					sc.append("=");
					sc.append(point.getDeviceCode());
					sc.append(",");
					sc.append(SENSOR_TAG);
					sc.append("=");
					sc.append(point.getSensorCode());
					sc.append(" ");
					sc.append(point.getTimestamp());
					sc.append(" ");
					sc.append("value");
					sc.append("=");
					sc.append(point.getValue());
					sc.append("\r\n");
				}
				fw.write(sc.toString());
				fw.close();
				count += tsFiles.size();
			}
		} catch (IOException e) {
			System.err.println("opentsdb 数据生成异常");
			e.printStackTrace();
			System.exit(0);
		}
		long endTime = System.nanoTime();
		System.out.println("数据生成器共消耗时间[" + (endTime - startTime) / 1000 + "]s");
		System.out.println("数据生成器共生成[" + count + "]条数据");
		System.out.println("数据生成器生成速度[" + (float) count / (endTime - startTime) * 1000 + "]points/s数据");
		return count;
	}

	@Override
	public Status selectMaxByDeviceAndSensor(String deviceCode, String sensorCode, Date startTime, Date endTime) {
		return selectAggrByDeviceAndSensor(AggreType.MAX, deviceCode, sensorCode, startTime, endTime);
	}

	@Override
	public Status selectMinByDeviceAndSensor(String deviceCode, String sensorCode, Date startTime, Date endTime) {
		return selectAggrByDeviceAndSensor(AggreType.MIN, deviceCode, sensorCode, startTime, endTime);
	}

	@Override
	public Status selectAvgByDeviceAndSensor(String deviceCode, String sensorCode, Date startTime, Date endTime) {
		return selectAggrByDeviceAndSensor(AggreType.AVG, deviceCode, sensorCode, startTime, endTime);
	}

	@Override
	public Status selectCountByDeviceAndSensor(String deviceCode, String sensorCode, Date startTime, Date endTime) {
		return selectAggrByDeviceAndSensor(AggreType.COUNT, deviceCode, sensorCode, startTime, endTime);
	}
	
	private Status selectAggrByDeviceAndSensor(String aggrType,String deviceCode, String sensorCode, Date startTime, Date endTime) {
	    Map<String,Object> map = new  HashMap<String,Object>();
		map.put("start",startTime.getTime());
		map.put("end", endTime.getTime());
		long tms=endTime.getTime()-startTime.getTime();
		Map<String,Object> subQuery = new HashMap<String ,Object>();
		subQuery.put("aggregator", "avg");//此只能用sum，不能用count，用count则结果出错
		subQuery.put("metric", METRIC);
		subQuery.put("downsample", tms+"ms-"+aggrType);
		Map<String,Object> subTag = new HashMap<String ,Object>();
		subTag.put(DEViCE_TAG,deviceCode);
		subTag.put(SENSOR_TAG,sensorCode);
		subQuery.put("tags", subTag);	
		List<Map<String,Object>> list = new ArrayList<>();
		list.add(subQuery);
		map.put("queries",list);
		String json=JSON.toJSONString(map);
		return queriesByHttpClient(json);
	}
}
