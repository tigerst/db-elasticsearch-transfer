package com.tiger.transfer.callback;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.druid.pool.DruidPooledPreparedStatement;
import com.tiger.transfer.Task.Db2EsByTimeTask;
import com.tiger.transfer.util.Constants;
import com.tiger.transfer.util.DateUtil;
import com.tiger.transfer.util.DbUtil;
import com.tiger.transfer.util.EsUtil;
import com.tiger.transfer.util.SqlUtil;

/**
 * 迁移回调接口实现类:BD -> ES
 * Mysql/Oracle -> elasticsearch
 * 
 * @ClassName: TransferCallbackImpl.java
 * @Description: 
 * @author: Tiger
 * @date: 2017年6月30日 下午5:29:52
 *
 */
public abstract class AbstractDb2EsByTimeCallback implements TransferCallback {

	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	protected int batchSize; // 批请求大小

	protected String esIndex; // 索引

	protected String esType; // 类型

	protected String idField; // id主键

	protected String curTmestampPattern; // 时间戳格式

	protected List<String> fields;	//字段列表

	protected volatile boolean isFirst = true; // 首次运行，首次运行分页查询

	protected int threadNums = 10; // 默认线程池数
	
	public AbstractDb2EsByTimeCallback() {
		esIndex = Constants.appConfig.getProperty("elasticsearch.index"); // 设置索引
		esType = Constants.appConfig.getProperty("elasticsearch.type"); // 设置类型
		idField = Constants.appConfig.getProperty("elasticsearch.id.field"); // 设置主键
		batchSize = NumberUtils.toInt(Constants.appConfig.getProperty("elasticsearch.batch.request")); // 设置主键
		curTmestampPattern = Constants.appConfig.getProperty("app.timestamp.pattern");
	}

	/**
	 * 功能：迁移数据
	 * 
	 * @param task	//传入任务类型
	 * @throws SQLException
	 * @throws IOException
	 */
	@Override
	public void transferData(Object task) throws SQLException, IOException {
		Db2EsByTimeTask pospesOracleTask = null;
		if (task instanceof Db2EsByTimeTask) {
			pospesOracleTask = (Db2EsByTimeTask) task;
		} else {
			logger.error("the task is not Db2EsByTimeTask");
			System.exit(1); // 退出应用
		}
		String prevTmestamp = pospesOracleTask.getCurTmestamp();
		String curTmestamp = DateUtil.formatDate(new Timestamp(System.currentTimeMillis()), curTmestampPattern); // 缓存执行sql之前的时间戳
		String sql = SqlUtil.getSql(); // 获取sql,使用sql防注入
		if (isFirst) { // 首次分批多线程处理，避免单线程性能瓶颈
			logger.info("the first transfer will be exectued by pages, and the total page : " + threadNums);
			transferByPage(pospesOracleTask, prevTmestamp, curTmestamp, sql);
			pospesOracleTask.setCurTmestamp(curTmestamp); // 缓存当前时间作为下一执行条件
			return;
		}
		transfer(prevTmestamp, curTmestamp, sql);
		pospesOracleTask.setCurTmestamp(curTmestamp); // 缓存当前时间作为下一执行条件
	}

	/**
	 * 分页迁移数据
	 * 
	 * @param pospesOracleTask
	 * @param prevTmestamp
	 * @param sql
	 */
	protected abstract void transferByPage(Db2EsByTimeTask db2EsByTimeTask, String prevTmestamp, String curTmestamp, String sql);

	
	/**
	 * 分页查询任务
	 * @ClassName: AbstractDb2EsByTimeCallback.java
	 * @Description: 
	 * @author: Tiger
	 * @date: 2017年7月2日 下午1:08:27
	 *
	 */
	protected class PageTask implements Runnable {
		
		private final CountDownLatch countDownLatch;

		private final String prevTmestamp;
		
		private final String curTmestamp;

		private final String sql;
		

		public PageTask(CountDownLatch countDownLatch, String prevTmestamp, String curTmestamp, String sql) {
			this.countDownLatch = countDownLatch;
			this.prevTmestamp = prevTmestamp;
			this.curTmestamp = curTmestamp;
			this.sql = sql;
		}

		@Override
		public void run() {
			try {
				AbstractDb2EsByTimeCallback.this.transfer(prevTmestamp, curTmestamp, sql);	//下界与上界设置
			} catch (SQLException | IOException e) {
				logger.error("transfer data error by Page", e);
				logger.error("the error task executed sql : " + sql);
			} finally {
				countDownLatch.countDown();
			}
		}

	}

	/**
	 * 获取该sql结果集总数
	 * 
	 * @param prevTmestamp
	 * @param sql
	 * @return
	 */
	protected long getTotal(String prevTmestamp, String curTmestamp,String sql) {
		long totalCnt = 0L;
		DruidPooledConnection conn = null;
		DruidPooledPreparedStatement ps = null;
		ResultSet rs = null;
		try {
			sql = sql.replace("$startTime", "'" + prevTmestamp + "'");
			sql = sql.replace("$endTime", "'" + curTmestamp + "'");
			StringBuffer sb = new StringBuffer("SELECT count(1) FROM ( ");
			sb.append(sql).append(" ) tmp");
			logger.debug("the executed sql : \n" + sb.toString());
			conn = DbUtil.getInstance().getConnection(); // 获得druid的连接
			ps = (DruidPooledPreparedStatement) conn.prepareStatement(sql);
			ps = (DruidPooledPreparedStatement) conn.prepareStatement(sb.toString());
			rs = ps.executeQuery();
			rs.next();
			totalCnt = rs.getLong(1); // 总数查询的第一个为总数达小
		} catch (Exception e) {
			logger.error("get total count by sql error", e);
		} finally {	//关闭资源
			DbUtil.getInstance().closeResource(rs, ps, conn);
		}
		return totalCnt;
	}

	/**
	 * 迁移数据
	 * sql的时间案例：$startTime(替换开始时间为上一次的执行时间) $endTime（替换结束时间为当前时间）
	 * @param prevTmestamp
	 * @param sql 
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	private void transfer(String prevTmestamp, String curTmestamp, String sql) throws SQLException, IOException {
		DruidPooledConnection conn = null;
		DruidPooledPreparedStatement ps = null;
		ResultSet rs = null;
		Client client = EsUtil.getInstance().getClient(); // 获取es连接
		try {
			sql = sql.replace("$startTime", "'" + prevTmestamp + "'");
			sql = sql.replace("$endTime", "'" + curTmestamp + "'");
			logger.debug("the executed sql : \n" + sql);
			conn = DbUtil.getInstance().getConnection(); // 获得druid的连接
			ps = (DruidPooledPreparedStatement) conn.prepareStatement(sql);
			logger.info("the previous executed timestamp : " + prevTmestamp + ", the current timestamp " + curTmestamp);
			rs = ps.executeQuery();
			if (fields == null || fields.size() <= 0)
				fields = SqlUtil.getFields(rs);
			client = EsUtil.getInstance().getClient();	//获取es连接
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			XContentBuilder xContentBuilder = null;
			String esId = null;
			int counter = 0;
			while (rs.next()) {
				esId = StringUtils.isEmpty(idField) ? null : rs.getString(idField);
				xContentBuilder = XContentFactory.jsonBuilder().startObject();// 构建json
				for (String field : fields) // 设置字段值
					xContentBuilder.field(field, rs.getString(field));
				xContentBuilder.endObject();
				bulkRequest.add(client.prepareIndex(esIndex, esType, esId).setSource(xContentBuilder));
				counter++;
				if (counter > batchSize) { // 大于1000提交请求并初始化
					bulkRequest.execute().actionGet(); // 提交请求
					counter = 0; // 清零
					bulkRequest = client.prepareBulk(); // 批请求
					xContentBuilder = null; // 置空
				}
			}
			if (xContentBuilder != null) // 提交最后的请求
				bulkRequest.execute().actionGet(); // 提交请求
		} finally {	//关闭资源
			DbUtil.getInstance().closeResource(rs, ps, conn);
		}
	}

}
