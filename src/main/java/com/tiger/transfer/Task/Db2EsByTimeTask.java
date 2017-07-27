package com.tiger.transfer.Task;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiger.transfer.callback.Mysql2EsByTimeCallbackImpl;
import com.tiger.transfer.callback.Oracle2EsByTimeCallbackImpl;
import com.tiger.transfer.callback.TransferCallback;
import com.tiger.transfer.util.Constants;
import com.tiger.transfer.util.DbUtil;
import com.tiger.transfer.util.EsUtil;

/**
 * 启动命令：
 * 格式：nonup java -classpath XXX.jar XXX config > stdout.out &
 * nohup java -classpath oes-transfer-1.0.0-jar-with-dependencies.jar com.lakala.ipos.inquiry.Task.PospEsOracleTask  file:///home/ipos/posp-oes-transfer/posp-oes.properties > stdout.out &
 * @ClassName: PospEsOracleTask.java
 * @Description: 
 * @author: Tiger
 * @date: 2017年6月29日 下午12:21:08
 *
 */
public class Db2EsByTimeTask {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private volatile String curTmestamp; // 当前时间戳字符串

	private volatile boolean isActive = false;	//运行标记
	
	private int interval; // 毫秒级别

	private TransferCallback callback;

	public void start() throws Exception {
		curTmestamp = Constants.appConfig.getProperty("app.starttime.init"); // 初始化时间戳
		interval = NumberUtils.toInt(Constants.appConfig.getProperty("task.interval.ms")); // 默认2000毫秒（2s）
		isActive = true; // 设置运行标记
		String dsType = DbUtil.getInstance().getDruidDataSource().getDbType();	//初始化创建连接
		if(StringUtils.equalsIgnoreCase(dsType, "oracle"))
			callback = new Oracle2EsByTimeCallbackImpl();	//初始化oracle回调接口
		else if(StringUtils.equalsIgnoreCase(dsType, "mysql"))
			callback = new Mysql2EsByTimeCallbackImpl();	//初始化mysql回调接口
		else 
			throw new RuntimeException("do not support this dbType");
		Thread thread = new Thread(new TransferTask());
		thread.start();
	}

	/**
	 * 任务类
	 * @ClassName: PospEsOracleTask.java
	 * @Description: 
	 * @author: Tiger
	 * @date: 2017年6月29日 上午10:02:00
	 *
	 */
	private class TransferTask implements Runnable {
		
		@Override
		public void run() {
			try {
				while (isActive) {
					callback.transferData(Db2EsByTimeTask.this);	//内部了获取外部类当前实例：外部类名.this
					Thread.sleep(interval); // 线程休眠
				}
			} catch (Throwable e) {
				logger.error("transfer data error ", e);
			} finally {	//线程异常或退出时关闭连接池
				DbUtil.getInstance().close();	//初始化创建连接
				EsUtil.getInstance().closeClient(); // 关闭连接
				logger.info("the application will exit");
				System.exit(1); // 停止程序
			}
		}
	}

	public String getCurTmestamp() {
		return curTmestamp;
	}

	public void setCurTmestamp(String curTmestamp) {
		this.curTmestamp = curTmestamp;
	}

}
