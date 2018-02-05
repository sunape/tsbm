package cn.edu.ruc.biz;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.ruc.TSUtils;
import cn.edu.ruc.db.DBBase;
import cn.edu.ruc.db.Status;
import cn.edu.ruc.db.TsPoint;

/**
 * 压力测试
 * @author sxg
 */
public class StressAppend {
	private static final Logger LOGGER=LoggerFactory.getLogger(StressAppend.class);
	private static  int THREAD_NUMBER=50;
	/**设备编号*/
	private static final List<String> DEVICE_CODES=Constants.DEVICE_CODES;
	/**传感器编号*/
	private static final List<String> SENSOR_CODES=Constants.SENSOR_CODES; 
	private static final Map<String,Long> SHIFT_TIME_MAP=Constants.SHIFT_TIME_MAP; 
	public static void main(String[] args) throws Exception {
		THREAD_NUMBER=SystemParam.APPEND_CLIENTS;
		initSensorCodes(SystemParam.APPEND_SENSOR_NUM);
		initSensorFunction();
		startStressAppend();
	}
	public static void startStressAppend() throws Exception{
		//1,生成dn个 设备(每个设备500个传感器)的7min的数据
		//dn=threads*k k>0;
		int seq=SystemParam.APPEND_MIN_DEV_NUM;
		long start=System.currentTimeMillis();
		while(seq<=SystemParam.APPEND_MAX_DEV_NUM){
			DBBase dbBase= Constants.getDBBase();
			long sumAllPoints=0;
			long sumAllTimeout=0;
			long sumTimeLoad=0;
			int cacheSize=SystemParam.APPEND_CACHE_NUM;
			int maxLoop=SystemParam.APPEND_LOOP;
			long virtualEndTime=System.currentTimeMillis();
			long virtualTime=virtualEndTime-SystemParam.APPEND_STEP*maxLoop*cacheSize;//
			ExecutorService pool = Executors.newFixedThreadPool(THREAD_NUMBER);
			CompletionService<Status> cs = new ExecutorCompletionService<Status>(pool);
			int currentLoop=0;
			while(currentLoop<maxLoop){
				LinkedList<List<TsPoint>> linkedTsPoints= generateAppendData(seq,THREAD_NUMBER,virtualTime,cacheSize);
				//开辟线程写入数据
				long sumTimeout=0L;
				long sumPoints=0L;
				long startTimeLoad=System.nanoTime();
				for(int threadIndex=0;threadIndex<THREAD_NUMBER;threadIndex++){
					List<TsPoint> points=linkedTsPoints.removeFirst();
					
					cs.submit(new Callable<Status>() {
						@Override
						public Status call() throws Exception {
							Status status = dbBase.insertMulti(points);
							return status;
						}
					});
				}
				for(int index=0;index<THREAD_NUMBER;index++) {
					Status status = cs.take().get();
					if(status.isOK()) {
						sumTimeout+=status.getCostTime();
						sumPoints+=status.getPointNum();
					}
				}
				long endTimeLoad=System.nanoTime();
				long costTime=endTimeLoad-startTimeLoad;
				int pps=(int)(sumPoints/(costTime/Math.pow(10, 9)));
				int timeout=(int) (sumTimeout/(double)sumPoints);//每个点的延迟时间，单位是us
				LOGGER.info("order[{}/{}],import[{}]points,cost [{} s],speed [{} points/s],timeout[{} us/kpoints]"
						,currentLoop+1,maxLoop,sumPoints,costTime/Math.pow(10, 9),pps,timeout);
				sumTimeLoad+=costTime;
				sumAllPoints+=sumPoints;
				sumAllTimeout+=sumTimeout;
				virtualTime+=SystemParam.APPEND_STEP*cacheSize;
				currentLoop++;
				Thread.sleep(300L);
			}
			LOGGER.info("device number="+seq*THREAD_NUMBER+",avg load speed "+(long)(sumAllPoints/(sumTimeLoad/Math.pow(10,9)))+" points/s,avg timeout ["+sumAllTimeout/sumAllPoints+" us/kpoints]");
			seq+=SystemParam.APPEND_INTERVAL_DEV_NUM;
		}
		long end=System.currentTimeMillis();
		System.out.println("costTime:"+(end-start));
	}
	/**
	 * 生成当前模拟时间的数据
	 * @param seq
	 * @param tHREAD_NUMBER2
	 * @param virtualTime
	 * @return
	 */
	private static LinkedList<List<TsPoint>> generateAppendData(int seq, int threadNums, long virtualTime,int cacheSize) {
		int dn=threadNums*seq;
		int sn=SystemParam.APPEND_SENSOR_NUM;
		double loseRatio=SystemParam.APPEND_LOSE_RATIO;
		LinkedList<List<TsPoint>> linkedPoints=new LinkedList<List<TsPoint>>();
		initDeviceCodes(dn);
		initShiftTime();
		Random r=new Random();
		for(int dnIndex=0;dnIndex<dn;dnIndex++){
			String deviceCode=DEVICE_CODES.get(dnIndex);
			List<TsPoint> points=null;
			if(linkedPoints.size()<threadNums) {
				points=new LinkedList<TsPoint>();
			}else {
				points=linkedPoints.removeFirst();
			}
			int index=0;
			while (index<cacheSize) {
				virtualTime+=index*SystemParam.APPEND_STEP;
				for(int sensorNum=0;sensorNum<sn;sensorNum++){//500为sensor总数
					double randomFloat = r.nextDouble();
					if(randomFloat<(1-loseRatio)){
						TsPoint point=new TsPoint();
						point.setDeviceCode(deviceCode);
						String sensorCode = SENSOR_CODES.get(sensorNum);
						point.setSensorCode(sensorCode);
						point.setValue(getValue(deviceCode,sensorCode,virtualTime));
						point.setTimestamp(virtualTime);
						points.add(point);
					}
				}
				index++;
			}
			linkedPoints.addLast(points);
		}
		return linkedPoints;
	}
	public static Object getValue(String deviceCode,String sensorCode,long currentTime){
		FunctionParam functionParam=Core.getFunctionBySensor(sensorCode);
		return Function.getValueByFuntionidAndParam(functionParam.getFunctionType(), functionParam.getMax(), functionParam.getMin(), functionParam.getCycle(),currentTime);
	}
	private static void initSensorFunction() {
		Core.initSensorFunction(SystemParam.APPEND_SENSOR_NUM);
	}
	private static void initSensorCodes(int sn) {
		SENSOR_CODES.clear();
		for(int i=0;i<sn;i++){
			String sensorCode="s_"+TSUtils.getRandomLetter(3)+"_"+i;
			SENSOR_CODES.add(sensorCode);
		}
	}
	private static void initDeviceCodes(int dn) {
		DEVICE_CODES.clear();
		for(int i=0;i<dn;i++){
			String deviceCode=UUID.randomUUID().toString().split("-")[0];
			DEVICE_CODES.add(deviceCode);
		}
	}
	/**
	 * 初始化时间偏移量
	 */
	private static void initShiftTime() {
		SHIFT_TIME_MAP.clear();
		int step=7000;
		long sensorSum=DEVICE_CODES.size()*SENSOR_CODES.size();
		Random r=new Random();
		for(int i=0;i<DEVICE_CODES.size();i++){
			for(int j=0;j<SENSOR_CODES.size();j++){
				Long shiftTime=(long)(r.nextDouble()*sensorSum)*step;
				SHIFT_TIME_MAP.put(DEVICE_CODES.get(i)+"_"+SENSOR_CODES.get(j), shiftTime);
			}
		}
	}
}

