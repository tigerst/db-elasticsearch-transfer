package com.tiger.transfer.util;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.druid.pool.DruidPooledPreparedStatement;

public class DbUtil {

	private static final Logger logger = LoggerFactory.getLogger(DbUtil.class);
	
	private DruidDataSource druidDataSource;
	
	private static DbUtil dbUtil = null;	//单例模式
	
	/**
	 * 私有化构造器
	 */
	private DbUtil() {
		
	}
	
	/**
	 * 提供公共获取实例对象
	 * @return
	 */
	public static DbUtil getInstance() {
		if(dbUtil == null)
			dbUtil = new DbUtil();
		return dbUtil;
	}
	
	/**
	 * 返回druid数据库连接
	 * @return
	 * @throws SQLException
	 */
	public DruidPooledConnection getConnection() throws SQLException{
		return this.druidDataSource.getConnection();
	}
	
	/**
	 * 获取单例datasource
	 * @return
	 */
	public DruidDataSource getDruidDataSource() {
		if(druidDataSource == null || druidDataSource.isClosed()){
			try {
				druidDataSource = newDataSource();	//初始化datasource
			} catch (Exception e) {
				logger.error("get collection error", e);
			}
		}
		return druidDataSource;
	}

	/**
	 * 创建数据库连接池
	 * @return
	 * @throws Exception
	 */
	private DruidDataSource newDataSource() throws Exception {
		logger.debug("create datasource...");
		DruidDataSource druidDataSource = new DruidDataSource();
		// druid数据库名
		druidDataSource.setName(Constants.appConfig.getProperty("datasource.name").trim());
		// 数据库url
		druidDataSource.setUrl(Constants.appConfig.getProperty("datasource.url").trim());
		// 数据库用户名
		druidDataSource.setUsername(Constants.appConfig.getProperty("datasource.username").trim());
		// 数据库密码
		druidDataSource.setPassword(Constants.appConfig.getProperty("datasource.password").trim());
		// 数据库驱动器类
		druidDataSource
				.setDriverClassName(Constants.appConfig.getProperty("datasource.driverClassName").trim());

		/*
		 * 配置初始化大小、最小、最大
		 */
		// 初始化大小
		druidDataSource.setInitialSize(
				Integer.parseInt(Constants.appConfig.getProperty("datasource.initialSize").trim()));
		// 线程池最大活动数
		druidDataSource
				.setMaxActive(Integer.parseInt(Constants.appConfig.getProperty("datasource.maxActive").trim()));
		// 线程池最小闲置数
		druidDataSource
				.setMinIdle(Integer.parseInt(Constants.appConfig.getProperty("datasource.minIdle").trim()));

		// 配置获取连接等待超时的时间
		druidDataSource
				.setMaxWait(Integer.parseInt(Constants.appConfig.getProperty("datasource.maxWait").trim()));
		// 配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
		druidDataSource.setTimeBetweenEvictionRunsMillis(Long
				.parseLong(Constants.appConfig.getProperty("datasource.timeBetweenEvictionRunsMillis").trim()));
		// 配置一个连接在池中最小生存的时间，单位是毫秒
		druidDataSource.setMinEvictableIdleTimeMillis(Long
				.parseLong(Constants.appConfig.getProperty("datasource.minEvictableIdleTimeMillis").trim()));
		// 用来检测连接是否有效的sql，要求是一个查询语句
		druidDataSource
				.setValidationQuery(Constants.appConfig.getProperty("datasource.validationQuery").trim());
		/*
		 * 检测连接
		 */
		druidDataSource.setTestWhileIdle(
				Boolean.parseBoolean(Constants.appConfig.getProperty("datasource.testWhileIdle").trim()));
		druidDataSource.setTestOnBorrow(
				Boolean.parseBoolean(Constants.appConfig.getProperty("datasource.testOnBorrow").trim()));
		druidDataSource.setTestOnReturn(
				Boolean.parseBoolean(Constants.appConfig.getProperty("datasource.testOnReturn").trim()));

		// 是否开启PSCache
		druidDataSource.setPoolPreparedStatements(Boolean
				.parseBoolean(Constants.appConfig.getProperty("datasource.poolPreparedStatements").trim()));
		// 指定每个连接上PSCache的大小
		druidDataSource.setMaxPoolPreparedStatementPerConnectionSize(Integer.parseInt(Constants.appConfig
				.getProperty("datasource.maxPoolPreparedStatementPerConnectionSize").trim()));
		// 配置监控统计拦截的filters
//		druidDataSource.setFilters(Constants.appConfig.getProperty("datasource.filters").trim());
		// 设置慢SQL记录
		druidDataSource.setConnectionProperties(
				Constants.appConfig.getProperty("datasource.connectionProperties").trim());
		// 设置公用监控数据，合并多个DruidDataSource的监控数据
		druidDataSource.setUseGlobalDataSourceStat(Boolean
				.parseBoolean(Constants.appConfig.getProperty("datasource.useGlobalDataSourceStat").trim()));
		druidDataSource.init(); // 初始化
		logger.debug("create datasource successfully");
		return druidDataSource;
	}

	/**
	 * 关闭连接
	 */
	public synchronized void close() {
		if(druidDataSource != null){
			druidDataSource.close();
		}
	}
	
	/**
	 * 关闭资源
	 * @param rs	结果集对象
	 * @param ps	事务对象
	 * @param conn	连接对象
	 */
	public void closeResource(ResultSet rs, DruidPooledPreparedStatement ps, DruidPooledConnection conn) {
		try {
			if (rs != null)
				rs.close();
			if (ps != null)
				ps.close();
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			logger.error("release db resource error", e);
		}
	}

}
