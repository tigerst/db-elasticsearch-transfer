package com.tiger.transfer.callback;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
import com.tiger.transfer.Task.Db2EsByIdTask;
import com.tiger.transfer.util.Constants;
import com.tiger.transfer.util.DbUtil;
import com.tiger.transfer.util.EsUtil;
import com.tiger.transfer.util.SqlUtil;

/**
 * 迁移回调接口实现类:BD -> ES Mysql/Oracle -> elasticsearch
 * 
 * @ClassName: TransferCallbackImpl.java
 * @Description:
 * @author: Tiger
 * @date: 2017年6月30日 下午5:29:52
 *
 */
public class Db2EsByIdCallbackImpl implements TransferCallback {

	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	protected int batchSize; // 批请求大小

	protected String esIndex; // 索引

	protected String esType; // 类型

	protected String idField; // id主键

	protected List<String> fields; // 字段列表

	protected volatile boolean isFirst = true; // 首次运行，首次运行分页查询

	protected int threadNums = 10; // 默认线程池数

	public Db2EsByIdCallbackImpl() {
		esIndex = Constants.appConfig.getProperty("elasticsearch.index"); // 设置索引
		esType = Constants.appConfig.getProperty("elasticsearch.type"); // 设置类型
		idField = Constants.appConfig.getProperty("elasticsearch.id.field"); // 设置主键
		batchSize = NumberUtils.toInt(Constants.appConfig.getProperty("elasticsearch.batch.request")); // 设置主键
	}

	/**
	 * 功能：迁移数据 如果上一次执行的最大Id大于等于当前最大Id，则不执行迁移功能，继续等待，知道有最新数据
	 * 
	 * @param task
	 *            //传入任务类型
	 * @throws SQLException
	 * @throws IOException
	 */
	@Override
	public void transferData(Object task) throws SQLException, IOException {
		Db2EsByIdTask pospesOracleTask = null;
		if (task instanceof Db2EsByIdTask) {
			pospesOracleTask = (Db2EsByIdTask) task;
		} else {
			logger.error("the task is not Db2EsByIdTask");
			System.exit(1); // 退出应用
		}
		long prevMaxId = pospesOracleTask.getCurMaxId();
		String sql = SqlUtil.getSql(); // 获取sql,使用sql防注入
		long curMaxId = getMaxId(prevMaxId, sql); // 缓存执行sql之前的时间戳
		if (prevMaxId >= curMaxId) { // 前一次的最大Id大于等于当前Id则继续等待，知道有新数据
			logger.info("there's no new or update data, the prevMaxId : " + prevMaxId);
			return;
		}
		if (isFirst) { // 首次分批多线程处理，避免单线程性能瓶颈
			logger.info("the first transfer will be exectued by pages, and the total page : " + threadNums);
			transferByPage(pospesOracleTask, prevMaxId, curMaxId, sql);
			pospesOracleTask.setCurMaxId(curMaxId); // 缓存当前maxId作为下一执行条件
			return;
		}
		transfer(prevMaxId, sql);
		pospesOracleTask.setCurMaxId(curMaxId); // 缓存当前时间作为下一执行条件
	}

	/**
	 * 获取当前最大的主键maxId
	 * 
	 * @param prevMaxId
	 * @param sql
	 * @return
	 * @throws SQLException 
	 */
	private long getMaxId(long prevMaxId, String sql) throws SQLException {
		long maxId = 0L;
		DruidPooledConnection conn = null;
		DruidPooledPreparedStatement ps = null;
		ResultSet rs = null;
		try {
			sql = sql.replace("$prevMaxId", prevMaxId + "");
			StringBuffer sb = new StringBuffer("SELECT MAX(ID) FROM ( ");
			sb.append(sql).append(" ) tmp");
			logger.debug("the executed sql : \n" + sb.toString());
			conn = DbUtil.getInstance().getConnection(); // 获得druid的连接
			ps = (DruidPooledPreparedStatement) conn.prepareStatement(sql);
			ps = (DruidPooledPreparedStatement) conn.prepareStatement(sb.toString());
			rs = ps.executeQuery();
			rs.next();
			maxId = rs.getLong(1); // 最大值查询的第一个为最大值大小
		} finally { // 释放资源
			DbUtil.getInstance().closeResource(rs, ps, conn);
		}
		return maxId;
	}

	/**
	 * 分页迁移数据 根据ID的范围
	 * 
	 * @param pospesOracleTask
	 * @param prevTmestamp
	 * @param sql
	 */
	protected void transferByPage(Db2EsByIdTask db2EsByIdTask, long prevMaxId, long curMaxId, String sql) {
		// 构造总页数查询
		long totalCnt = curMaxId - prevMaxId;
		if (totalCnt == 0L)
			return;
		// 分页大小
		long PageSize = totalCnt % threadNums == 0L ? totalCnt / threadNums : totalCnt / threadNums + 1; // 求出分页大小
		final StringBuffer sb = new StringBuffer();
		ExecutorService exec = Executors.newFixedThreadPool(threadNums);
		final CountDownLatch countDownLatch = new CountDownLatch(threadNums);
		for (long i = 1; i <= threadNums; i++) { // i代表页码
			// 构造分页查询
			sb.setLength(0); // 清空数据
			sb.append("SELECT * FROM ( ").append(sql).append(" ) tmp WHERE ID > ")
					.append((i - 1) * PageSize + prevMaxId + 1).append(" AND ID < ")
					.append(i * PageSize + prevMaxId + 1);
			exec.submit(new PageTask(countDownLatch, prevMaxId, sb.toString()));
		}
		try {
			countDownLatch.await();
			exec.shutdown();
		} catch (InterruptedException e) {

		}
		db2EsByIdTask.setCurMaxId(curMaxId); // 缓存当前时间作为下一执行条件
		isFirst = false; // 设置非首次查询
		logger.info("the page transfer treated successfully");
	}

	/**
	 * 分页查询任务
	 * 
	 * @ClassName: AbstractDb2EsByIdCallback.java
	 * @Description:
	 * @author: Tiger
	 * @date: 2017年7月2日 下午1:05:29
	 *
	 */
	protected class PageTask implements Runnable {

		private final CountDownLatch countDownLatch;

		private final long prevMaxId;

		private final String sql;

		public PageTask(CountDownLatch countDownLatch, long prevMaxId, String sql) {
			this.countDownLatch = countDownLatch;
			this.prevMaxId = prevMaxId;
			this.sql = sql;
		}

		@Override
		public void run() {
			try {
				Db2EsByIdCallbackImpl.this.transfer(prevMaxId, sql); // 下界设置
			} catch (SQLException | IOException e) {
				logger.error("transfer data error by Page", e);
				logger.error("the error task executed sql : " + sql);
			} finally {
				countDownLatch.countDown();
			}
		}

	}

	/**
	 * 迁移数据
	 * 
	 * @param prevTmestamp
	 * @param sql
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	private void transfer(long prevMaxId, String sql) throws SQLException, IOException {
		DruidPooledConnection conn = null;
		DruidPooledPreparedStatement ps = null;
		ResultSet rs = null;
		Client client = EsUtil.getInstance().getClient(); // 获取es连接
		try {
			sql = sql.replace("$prevMaxId", prevMaxId + "");
			logger.debug("the executed sql : \n" + sql);
			conn = DbUtil.getInstance().getConnection(); // 获得druid的连接
			ps = (DruidPooledPreparedStatement) conn.prepareStatement(sql);
			rs = ps.executeQuery();
			logger.info("the prevMaxId : " + prevMaxId);
			if (fields == null || fields.size() <= 0)
				fields = SqlUtil.getFields(rs);
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
