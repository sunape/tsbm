package cn.edu.ruc.biz;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.core.util.TimeUtil;
import cn.edu.ruc.TSUtils;
import cn.edu.ruc.TimeSlot;
import cn.edu.ruc.biz.db.BizDBUtils;
import cn.edu.ruc.biz.db.BizDBUtils2;
import cn.edu.ruc.biz.model.LoadRatio;
import cn.edu.ruc.db.DBBase;
import cn.edu.ruc.db.Status;
import cn.edu.ruc.db.TsPoint;
import cn.edu.ruc.enums.LoadTypeEnum;
import cn.edu.ruc.enums.TestMode;

/**
 * 
 * @author sxg
 */
public class Core {
	public static final String SYS_CONFIG_PATH="config_path";
	public static final String SYS_BINDING_PATH="bindings_path";
	public static final int SLEEP_TIMES=5000;
	private static final Logger LOGGER=LoggerFactory.getLogger(Core.class);
	private static final Random R=new Random();
	public static void main(String[] args) throws Exception {
		BizDBUtils2.initDataBase();
		initParam(args);
		initInnerFucntion();//初始化内置函数
//		BizDBUtils.createTables();
		BizDBUtils2.saveParam();
		switch (SystemParam.TEST_MODE) {
		case TestMode.IMPORT:
			DBBase dbBase = Constants.getDBBase();
			initConstant();
			loadPerformConCurrent(dbBase);
			break;
		case TestMode.APPEND:
			StressAppend.main(args);
			break;
		case TestMode.OVERFLOW:
			break;
		case TestMode.READ:
			initConstant();
			startStressUnAppend();
			break;
		case TestMode.MULTI:
			break;
		default:
			break;
		}
		Constants.getDBBase().cleanup();
		System.exit(0);
	}
	private static void initParam(String[] args) {
		Options options=new Options();
		Option config = Option.builder("cf").argName("cfg name").hasArg().desc("Config file path (optional)").build();
		Option bd = Option.builder("bd").argName("bd name").hasArg().desc("Binding file path (optional)").build();
		options.addOption(config);
		options.addOption(bd);
		CommandLineParser parser = new DefaultParser();
		try {
			CommandLine commandLine = parser.parse(options, args);
			String configPath=commandLine.getOptionValue("cf");
			String bingdingsPath=commandLine.getOptionValue("bd");
			System.setProperty(SYS_BINDING_PATH, bingdingsPath);
			System.setProperty(SYS_CONFIG_PATH, configPath);
			SystemParam.initParam();
			Properties prop=new Properties();
			prop.load(new FileInputStream(new File(bingdingsPath)));
			LOGGER.info(SystemParam.DB_TYPE);
			String dbClass = prop.getProperty(SystemParam.DB_TYPE);
			LOGGER.info(dbClass);
			SystemParam.DB_CLASS=dbClass;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	private static void startStressUnAppend() {
		int currentClients=0;
		int maxClients=SystemParam.READ_MAX_CLINETS;
		int minClients=SystemParam.READ_MIN_CLINETS;
		currentClients=minClients;
		while(currentClients<maxClients){
			currentClients++;
			execReadByClients(currentClients);//用currentClients客户端执行请求
		}
	}
	/**
	 * 用currentClients客户端执行请求
	 * @param currentClients
	 */
	private static void execReadByClients(int currentClients) {
		ExecutorService pool = Executors.newFixedThreadPool(currentClients);
		CompletionService<Long[]> cs = new ExecutorCompletionService<Long[]>(pool);
		long startTime=System.nanoTime();
		for( int index=0;index<currentClients;index++){
			cs.submit(new Callable<Long[]>() {
				@Override
				public Long[] call() throws Exception {
					Long[] results=new Long[2];
					long endTime=System.currentTimeMillis()+TimeUnit.SECONDS.toMillis(SystemParam.READ_TIMES);
					while(System.currentTimeMillis()<endTime) {
						Status status=execRead();
						if(status.isOK()) {
							results[0]=status.getCostTime();
							results[1]=results[1]+1;
						}
					}
					return results;
				}
			});
		}
		try {
			long endTime=System.nanoTime();
			long costTime=endTime-startTime;
			long sumTimeout=0L;
			long sumSuccessNum=0L;
			for(int index=0;index<currentClients;index++) {
				Long[] results = cs.take().get();
				sumTimeout+=results[0];
				sumSuccessNum+=results[1];
			}
			int tps=(int) (sumSuccessNum/(costTime/Math.pow(10, 9)));
			long avgtimeout=sumTimeout/sumSuccessNum;
			LOGGER.info("clients:{},throughput:[{} requests/sec],timeout [{} us/kpoints]",currentClients,tps,avgtimeout);
			Thread.sleep(TimeUnit.SECONDS.toMillis(3));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 执行读方法
	 * read 
	 * 缩量查询 每天中的每分钟的聚合值
	 * 聚合查询 某100分钟的聚合值
	 * 简单查询 查询1小时内的所有值
	 * @return
	 * @throws Exception 
	 */
	private static Status execRead() throws Exception {
		//FIXME
		//1 计算三类查询各个比例
		double aggreRatio = SystemParam.READ_AGGRE_RATIO;
		double shrinkRatio = SystemParam.READ_SHRINK_RATIO;
		double simpleRatio = SystemParam.READ_SIMPLE_RATIO;
		double sum=aggreRatio+shrinkRatio+simpleRatio;
		double aggre=aggreRatio/sum;
		double shrink=(aggreRatio+shrinkRatio)/sum;
		double simple=1.0;
		Random r=new Random();
		double nd = R.nextDouble();
		DBBase dbBase = Constants.getDBBase();
		Status status=null;
		String deviceCode = Core.getDeviceCodeByRandom();
		String sensorCode = Core.getSensorCodeByRandom();
		if(nd>=0&&nd<aggre) {
			//分析查询
			//FIXME 时间需要修正
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween("2016-03-01 00:00:00","2017-06-30 00:00:00",TimeUnit.HOURS.toMillis(24));
			status = dbBase.selectMaxByDeviceAndSensor(deviceCode, sensorCode, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}else if(nd>=aggre&&nd<shrink) {
			TsPoint point =new TsPoint();
			point.setDeviceCode(deviceCode);
			point.setSensorCode(sensorCode);
			//FIXME 时间需要修正
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween("2016-03-01 00:00:00","2017-06-30 00:00:00",TimeUnit.MINUTES.toMillis(60));
			status=dbBase.selectMinuteAvgByDeviceAndSensor(point,  new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
			//缩量查询
		}else if(nd>=shrink&&nd<=simple){
			//简单查询
			TsPoint point =new TsPoint();
			point.setDeviceCode(deviceCode);
			point.setSensorCode(sensorCode);
			//FIXME 时间需要修正
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween("2016-03-01 00:00:00","2017-06-30 00:00:00",TimeUnit.MINUTES.toMillis(100));
			status=dbBase.selectByDeviceAndSensor(point,  new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		//2 生成查询类型
		//3 执行查询
		return status;
	}
	/**
	 * 非加载数据 压力测试
	 * 线程数(用户数)从1不断增加到200
	 * 每个用户不断的发送请求 每次持续60s,并计算平均的响应数,每次间隔3s 
	 * 线程数用线程池管理,到60s后终止线程,进行统计
	 * @notice 目前不支持混合
	 */
	private static void startStressUnAppend(LoadTypeEnum loadTypeEnum) {
		DBBase dbBase = Constants.getDBBase();
		int currentClients=0;
		int maxClients=SystemParam.READ_MAX_CLINETS;
		int minClients=SystemParam.READ_MIN_CLINETS;
		currentClients=minClients;
		while(currentClients<maxClients){
			currentClients++;
			Map<Integer,Integer> countMap=new HashMap<Integer, Integer>();
			Map<Integer,Long> timeoutMap=new HashMap<Integer, Long>();
			ExecutorService pool = Executors.newFixedThreadPool(currentClients);
			CompletionService<Status> cs = new ExecutorCompletionService<Status>(pool);
			long startTime=System.currentTimeMillis();
			for( int index=0;index<currentClients;index++){
				final int thisIndex=index;
				countMap.put(thisIndex, 0);
				timeoutMap.put(thisIndex,0L);
				pool.execute(new Runnable() {
					@Override
					public void run() {
						//记录总响应时间，总操作数
						//执行请求操作 并计数
						Integer executeType = generateExecuteTypeByLoadType(loadTypeEnum);
						long currentTime=System.currentTimeMillis();
						while(currentTime-startTime<=TimeUnit.SECONDS.toMillis(10)){
							int count=countMap.get(thisIndex);
							executeType = generateExecuteTypeByLoadType(loadTypeEnum);
							Status status;
							try {
								status = execQueryByLoadType(dbBase, executeType);
								if(status.isOK()){
									countMap.put(thisIndex,++count);
									timeoutMap.put(thisIndex, status.getCostTime()+timeoutMap.get(thisIndex));
								}else{
									printlnErr("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"); 
								}
//								Thread.sleep(10);
								currentTime=System.currentTimeMillis();
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				});
			}
			pool.shutdown();
			try {
				pool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
				long currentTime=System.currentTimeMillis();
				double costTime = (currentTime-startTime)/Math.pow(1000.0, 1);
				Set<Integer> keySet = countMap.keySet();
				long sum=0;
				for(Integer key:keySet){
					sum+=countMap.get(key);
				}
				long timeoutSum=0;
				Set<Integer> keySet2 = timeoutMap.keySet();
				for(Integer key:keySet2){
					timeoutSum+=timeoutMap.get(key);
				}
				println("clients:"+currentClients+","+sum/costTime+" requests/sec,average timeout ["+(long)(timeoutSum/1000.0/sum)+" us/request]");
				Thread.sleep(TimeUnit.SECONDS.toMillis(3));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}


	/**
	 * 
	 * @return
	 */
	private static boolean initLoadAndPerformParam() {
//		Properties prop=Constants.PROPERTIES;
//		String dbClass = prop.getProperty(DB_CLASS_PROERTITY);
//		String dbUrl = Constants.getDBBase().getDBUrl();
//		Constants.initLoadTypeRatio();
//		Map<String, Object> map = BizDBUtils.selectLoadBatchByDbAndUrl(dbClass,dbUrl);
//		if(map==null){
//			printlnErr("please first load data ,start up program on mode online.load mode");
//			System.exit(0);
//		}
//		Constants.LOAD_BATCH_ID=((Number)map.get("id")).longValue();
//		Constants.DEVICE_NUMBER=(int) map.get("device_num");
//		Constants.SENSOR_NUMBER=(int) map.get("sensor_num");
//		Constants.POINT_STEP=((Number)map.get("point_step")).longValue();
//		Constants.POINT_LOSE_RATIO=(double) map.get("point_lose_ratio");
//		Constants.CACHE_POINT_NUM=(int) map.get("cache_point_num");
//		Constants.HISTORY_START_TIME=(long) map.get("history_start_time");
//		Constants.HISTORY_END_TIME=(long) map.get("history_end_time");
//		initDeviceSensorByDB();//初始化
//		String[] columns={"load_batch_id","write_ratio","simple_query_ratio","max_query_ratio","min_query_ratio",
//				"avg_query_ratio","count_query_ratio","sum_query_ratio","random_insert_ratio","update_ratio","insert_perform_device_prefix"};
//		Object[] values={Constants.LOAD_BATCH_ID,Constants.WRITE_RATIO,Constants.SIMPLE_QUERY_RATIO,Constants.MAX_QUERY_RATIO,Constants.MIN_QUERY_RATIO,
//					Constants.AVG_QUERY_RATIO,Constants.COUNT_QUERY_RATIO,Constants.SUM_QUERY_RATIO,Constants.RANDOM_INSERT_RATIO,Constants.UPDATE_RATIO,Constants.INSERT_PERFRM_DEVICE_PREFIX};
//		long performBatchId = BizDBUtils.insertBySqlAndParamAndTable(columns, values,"ts_perform_batch");
//		Constants.PERFORM_BATCH_ID=performBatchId;
		return true;
	}
	private static void initDeviceSensorByDB() {
		String deviceSql="select * from ts_device_info where load_batch_id=?";
		List<Map<String, Object>> deviceList = BizDBUtils.selectListBySqlAndParam(deviceSql,Constants.LOAD_BATCH_ID);
		for(Map<String,Object> device:deviceList){ 
			long deviceId=((Number) device.get("id")).longValue();
			String deviceName=(String) device.get("name");
			Constants.DEVICE_CODES.add(deviceName);
			String sensorSql="select * from ts_sensor_info where device_id=?";
			List<Map<String, Object>> sensors = BizDBUtils.selectListBySqlAndParam(sensorSql, deviceId);
			for(Map<String,Object> sensor:sensors){
				String sensorName=(String) sensor.get("name");
				String functionType=(String) sensor.get("function_type");
				String functionId=(String) sensor.get("function_id");
				long shiftTime=((Number) sensor.get("shift_time")).longValue();
				Constants.SHIFT_TIME_MAP.put(deviceName+"_"+sensorName, shiftTime);
				if(!Constants.SENSOR_CODES.contains(sensorName)){
					Constants.SENSOR_CODES.add(sensorName);
					Constants.SENSOR_FUNCTION.put(sensorName,Constants.getFunctionByFunctionTypeAndId(functionType,functionId));
				}
			}
		}
		Random r=new Random();
//		for(int j=0;j<Constants.SENSOR_NUMBER;j++){
//			Long shiftTime=(long)(r.nextDouble()*Constants.SENSOR_NUMBER)*Constants.POINT_STEP;
//			Constants.SHIFT_TIME_MAP.put(Constants.INSERT_PERFRM_DEVICE_PREFIX+"_"+Constants.SENSOR_CODES.get(j), shiftTime);
//		}
	}
	/**
	 * 本次测试基本信息保存到数据库中
	 */
	private static void saveLoadConstantToDB(Properties properties) {
		List<String> deviceCodes=Constants.DEVICE_CODES;
		for(String deviceCode:deviceCodes) {
			LOGGER.info(deviceCode);
		}
//		Long currentTime=System.currentTimeMillis();
//		Object[] params={Constants.DB_CLASS,currentTime,Constants.DEVICE_NUMBER,Constants.SENSOR_NUMBER,
//				Constants.POINT_STEP,Constants.CACHE_POINT_NUM,Constants.POINT_LOSE_RATIO,Constants.LINE_RATIO
//				,Constants.SIN_RATIO,Constants.SQUARE_RATIO,Constants.RANDOM_RATIO,Constants.CONSTANT_RATIO
//				,Constants.HISTORY_START_TIME,Constants.HISTORY_END_TIME,
//				1,Constants.getDBBase().getDBUrl()};
//		String[] batchColumns = {"target_db","create_time","device_num","sensor_num",
//							"point_step","cache_point_num","point_lose_ratio","line_ratio","sin_ratio",
//							"square_ratio","random_ratio","constant_ratio","history_start_time","history_end_time",
//							"data_status","db_url"};
//		long batchId = BizDBUtils.insertBySqlAndParamAndTable(batchColumns, params,"ts_load_batch");
//		Constants.LOAD_BATCH_ID=batchId;
//		List<String> deviceCodes=Constants.DEVICE_CODES;
//		Connection conn = BizDBUtils.getConnection();
//		try {
//			conn.setAutoCommit(false);
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//		for(String deviceCode:deviceCodes){
//			System.out.println("=============开始将设备["+deviceCode+"]写入数据库=============");
//			String[] columns={"create_time","name","load_batch_id"};
//			Object[] diParams={currentTime,deviceCode,Constants.LOAD_BATCH_ID};
//			long deviceId = BizDBUtils.insertBySqlAndParamAndTable(conn,columns, diParams, "ts_device_info");
//			List<String> sensorCodes=Constants.SENSOR_CODES;
//			for(String sensorCode:sensorCodes){
//				Long shiftTime=getShiftTimeByDeviceAndSensor(deviceCode, sensorCode);
//				FunctionParam function = getFunctionBySensor(sensorCode);
//				String[] sensorColumns={"create_time","name","device_id","function_id","function_type","shift_time"};
//				Object[] siParams={currentTime,sensorCode,deviceId,function.getId(),function.getFunctionType(),shiftTime};
//				BizDBUtils.insertBySqlAndParamAndTable(conn,sensorColumns, siParams,"ts_sensor_info");
//			}
//			System.out.println("=============结束将设备["+deviceCode+"]写入数据库=============");
//		}
//		try {
//			conn.commit();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//		BizDBUtils.closeConnection(conn);

	}
	public static void initConstant() {
//		initInnerFucntion();//初始化内置函数
		initDeviceCodes();//初始化设备编号
		initSensorCodes();//初始化传感器编号
		initShiftTime();//初始化时间偏移量
		initSensorFunction();//初始化传感器函数
		try {//初始化历史数据开始时间，结束时间
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween("2016-03-01 00:00:00","2017-06-30 00:00:00",TimeUnit.DAYS.toMillis(30));
			Constants.HISTORY_START_TIME=timeSlot.getStartTime();
			Constants.HISTORY_END_TIME=timeSlot.getEndTime();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	/**
	 * 打印help信息
	 */
	public static void usageMessage() {
		
	}
	
	//===================================================================
	//=======================service end=================================
	//===================================================================
	
	/**
	 * 导入数据测试
	 * 每秒钟可以导入多少个数据点
	 * 多线程
	 * @param dbBase
	 * @throws Exception
	 */
	public static void loadPerformConCurrent(DBBase dbBase) throws Exception {
		int sumTimes = dbBase.getSumTimes(SystemParam.IMPORT_CACHE_NUM*SystemParam.IMPORT_CLIENTS);
		long pointCountSum=0;
		long pointCostTimeSum=0;
		long costTimeSum=0L;
		println("total import["+sumTimes+"]times");
		ExecutorService pool = Executors.newFixedThreadPool(SystemParam.IMPORT_CLIENTS);
		CompletionService<Status> cs = new ExecutorCompletionService<Status>(pool);
		for(int i=0;i<sumTimes;i++){
			Map<String, List<TsPoint>> map = generateLoadDataMap(sumTimes, i+1);
			Set<String> deviceCodes = map.keySet();
			LinkedList<String> deviceCodeLink=new LinkedList<String>();
			for(String deviceCode:deviceCodes) {
				deviceCodeLink.add(deviceCode);
			}
			int deviceCodeNums=deviceCodes.size();

			long startTime=System.nanoTime();
			int pointSize=0;
			for(int threadId=0;threadId<SystemParam.IMPORT_CLIENTS;threadId++) {
				int deviceNumThisThread=deviceCodeNums/SystemParam.IMPORT_CLIENTS;
				if(threadId<(deviceCodeNums%SystemParam.IMPORT_CLIENTS)) {
					deviceNumThisThread++;
				}
				LinkedList<TsPoint> points=new LinkedList<TsPoint>();
				while(deviceNumThisThread>0) {
					points.addAll(map.get(deviceCodeLink.removeFirst()));
					deviceNumThisThread--;
				}
				if(points.size()>0) {
					cs.submit(new Callable<Status>() {
						@Override
						public Status call() throws Exception {
							Status status = dbBase.insertMulti(points);
							return status;
						}
					});
				}
			}
			long timeout=0L;
			for(int index=0;index<SystemParam.IMPORT_CLIENTS;index++) {
				Status status = cs.take().get();
				if(status.isOK()) {
					pointSize+=status.getPointNum();
					timeout+=status.getCostTime();
				}
			}
			long endime=System.nanoTime();
			long costNanoTime=endime-startTime;//消耗时间
			double costSecondTime=costNanoTime/Math.pow(10.0, 9);
			//平均每kpoints响应时间 us
			int timeoutPerKPoints=(int) ((double)timeout/pointSize);
			//每秒写入条数 points/s
			int pps=(int) (pointSize/costSecondTime);
			String log="[%s/%s] import[%s points],pps[%s points/sec],timeout[%s us/kpoints]";
			String format = String.format(log,i+1,sumTimes,pointSize,pps,timeoutPerKPoints);
			LOGGER.info(format);
			pointCountSum+=pointSize;
			costTimeSum+=costNanoTime;
			pointCostTimeSum+=timeout;
		}
		double avgTimeout=(double)pointCostTimeSum/pointCountSum;
		double programRatio=pointCountSum/(costTimeSum/Math.pow(1000.0, 3));
		System.out.println("total import["+sumTimes+"]times");
		System.out.println("total import["+pointCountSum+"]points");
		System.out.println("total cost["+TimeUnit.NANOSECONDS.toMillis(costTimeSum)+" ms]");
		System.out.println("average timeout["+(long)avgTimeout+" us/kps]");
		System.out.println("average speed["+(long)programRatio+" points/s]");
	}
	/**
	 * 结束施加负载
	 * @param flagMap 是否启动标识，key status value:true/false
	 */
	public static void endLoad(Map<String,Boolean> flagMap){
		flagMap.put("status", false);
	}
	/**
	 * 版本三，测试响应时间
	 * @param dbBase
	 * @param loadType
	 * @return
	 * @throws Exception
	 */
	public static  Status execQueryByLoadType(DBBase dbBase,Integer loadType) throws Exception{
		Status status=null;
		TsPoint point=new TsPoint();
		Integer internal=24*60;//一天
		String deviceCode = Core.getDeviceCodeByRandom();
		String sensorCode = Core.getSensorCodeByRandom();
		point.setDeviceCode(deviceCode);
		point.setSensorCode(sensorCode);
		//FIXME 查询时间段可优化
		if(LoadTypeEnum.WRITE.getId().equals(loadType)){
			//选择当前时间 插入数据
			List<TsPoint> points = Core.generateInsertData(1);
			status = dbBase.insertMulti(points);
		}
		if(LoadTypeEnum.RANDOM_INSERT.getId().equals(loadType)){
			//随机选择15分钟   HISTORY_START_TIME之前的 
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(TSUtils.getTimeByDateStr("2016-02-15 00:00:00"),Constants.HISTORY_START_TIME,TimeUnit.SECONDS.toMillis(6));
			//生成数据6s，进行写入
			List<TsPoint> points = generateDataBetweenTime(timeSlot.getStartTime(),timeSlot.getEndTime());
			status=dbBase.insertMulti(points);
		}
		if(LoadTypeEnum.UPDATE.getId().equals(loadType)){
			//随机选择6s   -HISTORY_START_TIME 取模为0的
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME,Constants.HISTORY_END_TIME,TimeUnit.SECONDS.toMillis(6));//时间可调整
			List<TsPoint> points = generateDataBetweenTime(timeSlot.getStartTime(),timeSlot.getEndTime());
			status=dbBase.updatePoints(points);
		}
		if(LoadTypeEnum.SIMPLE_READ.getId().equals(loadType)){
			//查一天的数据
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.SECONDS.toMillis(360));
			status = dbBase.selectByDeviceAndSensor(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(LoadTypeEnum.AGGRA_READ.getId().equals(loadType)){
			//查一天的数据的最大值
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.SECONDS.toMillis(3600));
			status = dbBase.selectMaxByDeviceAndSensor(deviceCode, sensorCode, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
//			status = dbBase.selectHourAvgByDevice(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(LoadTypeEnum.SHRINK_READ.getId().equals(loadType)){
			//查一天的数据的最大值
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.SECONDS.toMillis(3600*48));
			status = dbBase.selectHourMinByDevice(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		return status;
	}
	
	/**
	 * 生成某个时间段内的数据
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public static  List<TsPoint> generateDataBetweenTime(long startTime,long endTime){
		//单线程生成
		List<TsPoint> points=new ArrayList<TsPoint>();
		int deviceSum=SystemParam.IMPORT_DEV_NUM;
		int sensorSum=SystemParam.IMPORT_SENSOR_NUM;
		long step=SystemParam.IMPORT_STEP;
		double loseRatio=SystemParam.IMPORT_LOSE_RATIO;
		long current=0;
		Random r=new Random();
		for(long currentTime=startTime;currentTime<=endTime;){
			for(int deviceNum=0;deviceNum<deviceSum;deviceNum++){
				String deviceCode=Constants.DEVICE_CODES.get(deviceNum);
				for(int sensorNum=0;sensorNum<sensorSum;sensorNum++){
					double randomFloat = r.nextDouble();
					if(randomFloat<(1-loseRatio)){
						TsPoint point=new TsPoint();
						point.setDeviceCode(deviceCode);
						String sensorCode = Constants.SENSOR_CODES.get(sensorNum);
						point.setSensorCode(sensorCode);
						point.setValue(Core.getValue(deviceCode,sensorCode,currentTime));
						point.setTimestamp(currentTime);
						points.add(point);
					}
					current++;
					if(current%100000==0){
						System.out.println(current);
					}
				}
			}
			currentTime+=step;
		}
		return points;
	}
	
	/**
	 * 生成实时写入数据
	 * 
	 * @return
	 */
	public static List<TsPoint> generateInsertData(Integer deviceNum){
		long currentTime=System.currentTimeMillis();
		int sensorSum=SystemParam.APPEND_SENSOR_NUM;
		String deviceCode=Constants.INSERT_PERFRM_DEVICE_PREFIX;
		List<TsPoint> points=new ArrayList<TsPoint>();
		for(int i=0;i<deviceNum;i++){
			for(int j=0;j<sensorSum;j++){
				TsPoint point=new TsPoint();
				point.setDeviceCode(deviceCode+"_"+i);
				String sensorCode = Constants.SENSOR_CODES.get(j);
				point.setSensorCode(sensorCode);
				point.setValue(getValue(deviceCode,sensorCode,currentTime));
				point.setTimestamp(currentTime);
				points.add(point);
			}
		}
		return points;
	}
	/**
	 * 所有历史数据分批生成，根据当前次数和总次数生成数据
	 * 比如总共生成一个月的数据
	 * sumTimes=30,order=10
	 * 表示总共调用生成30次，现在是第10生成
	 * @param sumTimes 生成数据总调用次数
	 * @param order  当前次数 从1开始
	 * @return
	 */
	public static List<TsPoint> generateLoadData(int sumTimes,int order){
		if(sumTimes<1){
			System.out.println("生成数据时，sumTimes必须大于或者等于1");
			System.exit(0);
		}
		if(order<1){
			System.out.println("生成数据时，order必须大于或者等于1");
			System.exit(0);
		}
		if(order>sumTimes){
			System.out.println("生成数据时，sumTimes必须大于或者等于order");
			System.exit(0);
		}
		//单线程生成
		List<TsPoint> points=new ArrayList<TsPoint>();
		int deviceSum=SystemParam.IMPORT_DEV_NUM;
		int sensorSum=SystemParam.IMPORT_SENSOR_NUM;
		long step=SystemParam.IMPORT_STEP;
		double loseRatio=SystemParam.IMPORT_LOSE_RATIO;
		
		
		
		long startTime=(long) (Constants.HISTORY_START_TIME+
				(Constants.HISTORY_END_TIME-Constants.HISTORY_START_TIME)*((double)(order-1)/sumTimes));
		long endTime=(long) (Constants.HISTORY_START_TIME+
				(Constants.HISTORY_END_TIME-Constants.HISTORY_START_TIME)*((double)(order)/sumTimes));
		long current=0;
		Random r=new Random();
		for(long currentTime=startTime;currentTime<=endTime;){
			for(int deviceNum=0;deviceNum<deviceSum;deviceNum++){
				String deviceCode=Constants.DEVICE_CODES.get(deviceNum);
				for(int sensorNum=0;sensorNum<sensorSum;sensorNum++){
					double randomFloat = r.nextDouble();
					if(randomFloat<(1-loseRatio)){
						TsPoint point=new TsPoint();
						point.setDeviceCode(deviceCode);
						String sensorCode = Constants.SENSOR_CODES.get(sensorNum);
						point.setSensorCode(sensorCode);
						point.setValue(getValue(deviceCode,sensorCode,currentTime));
						point.setTimestamp(currentTime);
						points.add(point);
					}
					current++;
					if(current%500000==0){
						println(current+"");
					}
				}
			}
			currentTime+=step;
		}
		return points;
	}
	/**
	 * 生成数据
	 * key 设备号  value该设备号对应的数据
	 * @param sumTimes
	 * @param order
	 * @return
	 */
	public static Map<String,List<TsPoint>> generateLoadDataMap(int sumTimes,int order){
		if(sumTimes<1){
			System.out.println("生成数据时，sumTimes必须大于或者等于1");
			System.exit(0);
		}
		if(order<1){
			System.out.println("生成数据时，order必须大于或者等于1");
			System.exit(0);
		}
		if(order>sumTimes){
			System.out.println("生成数据时，sumTimes必须大于或者等于order");
			System.exit(0);
		}
		int deviceSum=SystemParam.IMPORT_DEV_NUM;
		int sensorSum=SystemParam.IMPORT_SENSOR_NUM;
		long step=SystemParam.IMPORT_STEP;
		double loseRatio=SystemParam.IMPORT_LOSE_RATIO;
		long startTime=(long) (Constants.HISTORY_START_TIME+
				(Constants.HISTORY_END_TIME-Constants.HISTORY_START_TIME)*((double)(order-1)/sumTimes));
		long endTime=(long) (Constants.HISTORY_START_TIME+
				(Constants.HISTORY_END_TIME-Constants.HISTORY_START_TIME)*((double)(order)/sumTimes));
		long current=0;
		Map<String,List<TsPoint>> map=new HashMap<String, List<TsPoint>>();
		for(long currentTime=startTime;currentTime<=endTime;){
			for(int deviceNum=0;deviceNum<deviceSum;deviceNum++){
				String deviceCode=Constants.DEVICE_CODES.get(deviceNum);
				List<TsPoint> points = map.get(deviceCode);
				if(points==null){
					points=new ArrayList<TsPoint>();
				}
				for(int sensorNum=0;sensorNum<sensorSum;sensorNum++){
					double randomFloat = RANDOM.nextDouble();
					if(randomFloat<(1-loseRatio)){
						TsPoint point=new TsPoint();
						point.setDeviceCode(deviceCode);
						String sensorCode = Constants.SENSOR_CODES.get(sensorNum);
						point.setSensorCode(sensorCode);
						point.setValue(getValue(deviceCode,sensorCode,currentTime));
						point.setTimestamp(currentTime);
						points.add(point);
					}
					current++;
					if(current%500000==0){
						println(current+"");
					}
				}
				
				map.put(deviceCode,points);
			}
			currentTime+=step;
		}
		return map;
	}
	/**
	 * 通过设备号和传感器号获取
	 * @param deviceCode
	 * @param sensorCode
	 * @return
	 */
	public static Object getValue(String deviceCode, String sensorCode,Long time) {
		long shirtTime=getShiftTimeByDeviceAndSensor(deviceCode,sensorCode);
		FunctionParam functionParam=getFunctionBySensor(sensorCode);
		return Function.getValueByFuntionidAndParam(functionParam.getFunctionType(), functionParam.getMax(), functionParam.getMin(), functionParam.getCycle(), time+shirtTime);
	}
	
	
	
	//===================================================================
	//=======================service end=================================
	//===================================================================
	
	
	/**
	 * 根据传感器编号获取传感器基本函数
	 * @param sensorCode
	 * @return
	 */
	public static FunctionParam getFunctionBySensor(String sensorCode) {
		return Constants.SENSOR_FUNCTION.get(sensorCode);
	}
	/**
	 * 根据设备和传感器获取时间偏移量
	 * 目的：防止同一个时间相同函数所生成的数据值一样
	 * @param deviceCode
	 * @param sensorCode
	 * @return
	 */
	private static long getShiftTimeByDeviceAndSensor(String deviceCode,
			String sensorCode) {
		Long shiftTime = Constants.SHIFT_TIME_MAP.get(deviceCode+"_"+sensorCode);
		if(shiftTime==null){
			shiftTime=0L;
		}
		return shiftTime;
	}
	/**
	 * 根据传感器数，初始化传感器编号
	 * @param sensorSum
	 * @return
	 */
	private static List<String> initSensorCodes() {
		for(int i=0;i<SystemParam.IMPORT_SENSOR_NUM;i++){
//			String sensorCode="s_"+TSUtils.getRandomLetter(3)+"_"+i;
			String sensorCode="s_"+i;
			Constants.SENSOR_CODES.add(sensorCode);
		}
		return Constants.SENSOR_CODES;
	}
	/**
	 * 根据设备数，初始化设备编号
	 * @param deviceSum
	 * @return
	 */
	private static List<String> initDeviceCodes() {
		for(int i=0;i<SystemParam.IMPORT_DEV_NUM;i++){
//			String deviceCode="d_"+TSUtils.getRandomLetter(2)+"_"+i;
			String deviceCode="d_"+i;
			Constants.DEVICE_CODES.add(deviceCode);
		}
		return Constants.DEVICE_CODES;
	}
	/**
	 * 初始化时间偏移量
	 */
	private static void initShiftTime() {
		long sensorSum=SystemParam.IMPORT_DEV_NUM*SystemParam.IMPORT_SENSOR_NUM;
		Random r=new Random();
		for(int i=0;i<SystemParam.IMPORT_DEV_NUM;i++){
			for(int j=0;j<SystemParam.IMPORT_SENSOR_NUM;j++){
				Long shiftTime=(long)(r.nextDouble()*sensorSum)*SystemParam.IMPORT_SENSOR_NUM;
				Constants.SHIFT_TIME_MAP.put(Constants.DEVICE_CODES.get(i)+"_"+Constants.SENSOR_CODES.get(j), shiftTime);
			}
		}
		for(int j=0;j<SystemParam.IMPORT_SENSOR_NUM;j++){
			Long shiftTime=(long)(r.nextDouble()*sensorSum)*SystemParam.IMPORT_STEP;
			Constants.SHIFT_TIME_MAP.put(Constants.INSERT_PERFRM_DEVICE_PREFIX+"_"+Constants.SENSOR_CODES.get(j), shiftTime);
		}
	}
	/**.
	 * 初始化内置函数
	 * functionParam
	 */
	public static void initInnerFucntion() {
		
		FunctionXml xml=null;
		try {
			InputStream input = Core.class.getResourceAsStream("function.xml");
			JAXBContext context = JAXBContext.newInstance(FunctionXml.class,FunctionParam.class);
			Unmarshaller unmarshaller = context.createUnmarshaller(); 
			xml = (FunctionXml)unmarshaller.unmarshal(input);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		List<FunctionParam> xmlFuctions = xml.getFunctions();
		for(FunctionParam param:xmlFuctions){
			if(param.getFunctionType().indexOf("-mono-k")!=-1){
				Constants.LINE_LIST.add(param);
			}else if(param.getFunctionType().indexOf("-mono")!=-1){
				//如果min==max则为常数，系统没有非常数的
				if(param.getMin()==param.getMax()){
					Constants.CONSTANT_LIST.add(param);
				}
			}else if(param.getFunctionType().indexOf("-sin")!=-1){
				Constants.SIN_LIST.add(param);
			}else if(param.getFunctionType().indexOf("-square")!=-1){
				Constants.SQUARE_LIST.add(param);
			}else if(param.getFunctionType().indexOf("-random")!=-1){
				Constants.RANDOM_LIST.add(param);
			}
		}
	}
	public static void initSensorFunction(int sensorNum) {
		//根据传进来的各个函数比例进行配置
		double sumRatio=SystemParam.FUNCTION_CONSTANT_RATIO+SystemParam.FUNCTION_LINE_RATIO+SystemParam.FUNCTION_RANDOM_RATIO+SystemParam.FUNCTION_SIN_RATIO+SystemParam.FUNCTION_SQUARE_RATIO;
		if(sumRatio!=0
			&&SystemParam.FUNCTION_CONSTANT_RATIO>=0
			&&SystemParam.FUNCTION_LINE_RATIO>=0
			&&SystemParam.FUNCTION_RANDOM_RATIO>=0
			&&SystemParam.FUNCTION_SIN_RATIO>=0
			&&SystemParam.FUNCTION_SQUARE_RATIO>=0){
			double constantArea=SystemParam.FUNCTION_CONSTANT_RATIO/sumRatio;
			double lineArea=constantArea+SystemParam.FUNCTION_LINE_RATIO/sumRatio;
			double randomArea=lineArea+SystemParam.FUNCTION_RANDOM_RATIO/sumRatio;
			double sinArea=randomArea+SystemParam.FUNCTION_SIN_RATIO/sumRatio;
			double squareArea=sinArea+SystemParam.FUNCTION_SQUARE_RATIO/sumRatio;
			Random r=new Random();
			for(int i=0;i<sensorNum;i++){
				double property = r.nextDouble();
				FunctionParam param=null;
				Random fr=new Random();
				double middle = fr.nextDouble();
				if(property>=0&&property<constantArea){//constant
					int index=(int)(middle*Constants.CONSTANT_LIST.size());
					param=Constants.CONSTANT_LIST.get(index);
				}
				if(property>=constantArea&&property<lineArea){//line
					int index=(int)(middle*Constants.LINE_LIST.size());
					param=Constants.CONSTANT_LIST.get(index);
				}
				if(property>=lineArea&&property<randomArea){//random
					int index=(int)(middle*Constants.RANDOM_LIST.size());
					param=Constants.RANDOM_LIST.get(index);
				}
				if(property>=randomArea&&property<sinArea){//sin
					int index=(int)(middle*Constants.SIN_LIST.size());
					param=Constants.SIN_LIST.get(index);
				}
				if(property>=sinArea&&property<squareArea){//square
					int index=(int)(middle*Constants.SQUARE_LIST.size());
					param=Constants.SQUARE_LIST.get(index);
				}
				if(param==null){
					System.err.println(" initSensorFunction() 初始化函数比例有问题！");
					System.exit(0);
				}
				String sensorCode = Constants.SENSOR_CODES.get(i);
				Constants.SENSOR_FUNCTION.put(sensorCode,param);
				
			}
			
		}else{
			System.err.println("function ratio must >=0 and sum>0");
			System.exit(0);
		}
	}
	/**
	 * 初始化传感器函数
	 * Constants.SENSOR_FUNCTION
	 */
	public static void initSensorFunction() {
		initSensorFunction(SystemParam.IMPORT_SENSOR_NUM);
	}
	/**
	 * 从已有传感器名称中获取一个传感器名称 
	 */
	public static String getSensorCodeByRandom() {
		List<String> sensors = Constants.getAllSensors();
		int size = sensors.size();
		Random r=new Random();
		double random=r.nextDouble()*size;
		String sensorCode = sensors.get((int)random);
		return sensorCode;
	}
	/**
	 * 从已有设备名称列表中获取一个设备名称
	 */
	public static String getDeviceCodeByRandom() {
		List<String> devices = Constants.getAllDevices();
		int size = devices.size();
		Random r=new Random();
		double random=r.nextDouble()*size;
		String deviceCode = devices.get((int)random);
		return deviceCode;
	}
	public static void println(String str){
		System.out.println(str);
	}
	public static void printlnErr(String str){
		System.err.println(str);
	}
	public static void printlnSplitLine(String str){
		int total=80;
		if(StringUtils.isBlank(str)){
			println(generateSymbol("=",total));
		}else{
			int length = str.length();
			if(length%2==0){
				println(generateSymbol("=",(total-length)/2)+str+generateSymbol("=",(total-length)/2));
			}else{
				println(generateSymbol("=",(total-length)/2)+str+generateSymbol("=",(total-length)/2+1));
			}
		}
	}
	public static void printlnSplitLine(){
		printlnSplitLine("");
	}
	/**
	 * 生成num个symbol
	 * @param symbol
	 * @param num
	 * @return
	 */
	private static String generateSymbol(String symbol,int num){
		StringBuilder sc=new StringBuilder();
		for(int i=0;i<num;i++){
			sc.append(symbol);
		}
		return sc.toString();
	}
	/**
	 * 打印进度
	 * @param current
	 * @param sum
	 * @param currentPercent
	 * @return
	 */
	private static int printProgress(int current,int sum,int currentPercent){
		int percent=(int)((double)current/sum*100);
		if(percent>currentPercent){
			if(currentPercent==0){
				System.out.println("");
			}else{
				System.out.print("\b\b\b");
			}
			if(currentPercent>9){
				System.out.print("\b");
			}
			System.out.print("=>"+percent+"%");
			currentPercent=percent;
			if(currentPercent==99){
				System.out.println("");
			}
		}
		return percent;
	}
	private static final Random RANDOM=new Random();
	/**
	 * 
	 * @param loadType
	 * @return
	 */
	private static Integer generateExecuteTypeByLoadType(
			final LoadTypeEnum loadType) {
		Integer executeType=0;//当前线程的操作类型
		LoadRatio loadRatio=LoadRatio.newInstanceByLoadType(loadType.getId());
		if(LoadTypeEnum.MUILTI.getId().equals(loadType.getId())){
			double rd = RANDOM.nextDouble();
			if(rd>=loadRatio.getWriteStartRatio()&&rd<loadRatio.getWriteEndRatio()){
				executeType=LoadTypeEnum.WRITE.getId();
			}
			if(rd>=loadRatio.getRandomInsertStartRatio()&&rd<loadRatio.getRandomInsertEndRatio()){
				executeType=LoadTypeEnum.RANDOM_INSERT.getId();
			}
			if(rd>=loadRatio.getUpdateStartRatio()&&rd<loadRatio.getUpdateEndRatio()){
				executeType=LoadTypeEnum.UPDATE.getId();
			}
			if(rd>=loadRatio.getSimpleQueryStartRatio()&&rd<loadRatio.getSimpleQueryEndRatio()){
				executeType=LoadTypeEnum.SIMPLE_READ.getId();
			}
			if(rd>=loadRatio.getAggrQueryStartRatio()&&rd<loadRatio.getAggrQueryEndRatio()){
				executeType=LoadTypeEnum.AGGRA_READ.getId();
			}
		}else{
			executeType=loadType.getId();
		}
		return executeType;
	}
}

