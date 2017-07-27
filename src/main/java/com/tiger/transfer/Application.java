package com.tiger.transfer;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiger.transfer.Task.Db2EsByIdTask;
import com.tiger.transfer.Task.Db2EsByTimeTask;
import com.tiger.transfer.util.Constants;
import com.tiger.transfer.util.DbUtil;
import com.tiger.transfer.util.EsUtil;
import com.tiger.transfer.util.PropertiesUtil;

/**
 * 启动类
 * 区分time model和ID model
 * @ClassName: Application.java
 * @Description: 
 * @author: Tiger
 * @date: 2017年7月2日 下午2:33:48
 *
 */
public class Application {

	private static final Logger logger = LoggerFactory.getLogger(Application.class);

	public static void main(String[] args) throws Exception {
		if(ArrayUtils.isNotEmpty(args))
			PropertiesUtil.loadProperties(args[0]);
		else
			PropertiesUtil.loadProperties(null);
		EsUtil.getInstance().getClient(); 	//取得连接
		DbUtil.getInstance().getDruidDataSource();	//初始化创建连接
		boolean startFlag = false;	//应用启动标记
		try {
			startFlag = true;
			String sqlModel = Constants.appConfig.getProperty("app.sql.model");
			if(StringUtils.equalsIgnoreCase(sqlModel, "0")) {		//time model,根据时间实现实时同步数据
				Db2EsByTimeTask db2EsByTimeTask = new Db2EsByTimeTask();
				db2EsByTimeTask.start();
			} else if(StringUtils.equalsIgnoreCase(sqlModel, "1")) {	//ID model,根据ID实现实时同步数据
				Db2EsByIdTask db2EsByIdTask = new Db2EsByIdTask();
				db2EsByIdTask.start();
			} else {	//其他模式暂不支持
				startFlag = false;
				throw new RuntimeException("Do not support this sqlModel");
			}
		} catch (Exception e) {
			logger.error("start application error", e);
		} finally {
			if(startFlag)
				logger.info("the application started successfully");
		}
	}
	
}