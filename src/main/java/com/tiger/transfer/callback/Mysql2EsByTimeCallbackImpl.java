package com.tiger.transfer.callback;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.tiger.transfer.Task.Db2EsByTimeTask;

/**
 * 迁移回调接口实现类:BD -> ES Mysql -> elasticsearch
 * 
 * @ClassName: TransferCallbackImpl.java
 * @Description:
 * @author: Tiger
 * @date: 2017年6月30日 下午5:29:52
 *
 */
public class Mysql2EsByTimeCallbackImpl extends AbstractDb2EsByTimeCallback {

	/**
	 * 分页迁移数据
	 * 
	 * @param mysql2EsTask
	 * @param prevTmestamp
	 * @param sql
	 */
	protected void transferByPage(Db2EsByTimeTask mysql2EsTask, String prevTmestamp, String curTmestamp, String sql) {
		// 构造总页数查询
		long totalCnt = getTotal(prevTmestamp, curTmestamp, sql);
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
			//分页sql组装
			sb.append("SELECT * FROM ( ").append(sql).append(" ) limit ") // 小于最大值
			.append((i - 1) * PageSize) // startIndex
			.append(" , ")
			.append(PageSize); // rows
			exec.submit(new PageTask(countDownLatch, prevTmestamp, curTmestamp, sb.toString()));
		}
		try {
			countDownLatch.await();
			exec.shutdown();
		} catch (InterruptedException e) {
			
		}
		mysql2EsTask.setCurTmestamp(curTmestamp); // 缓存当前时间作为下一执行条件
		isFirst = false; // 设置非首次查询
		logger.info("the page transfer treated successfully");
	}

}
