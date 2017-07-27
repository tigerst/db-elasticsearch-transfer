package com.tiger.transfer.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.lang3.math.NumberUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EsUtil {

	private final Logger logger = LoggerFactory.getLogger(EsUtil.class);
	
	private Client client; // 客户端连接对象
	
	private static EsUtil esUtil = null;	//单例对象
	
	/**
	 * 私有化构造器
	 */
	private EsUtil() {
		
	}
	
	/**
	 * 提供公共获取实例对象
	 * @return
	 */
	public static EsUtil getInstance() {
		if(esUtil == null) 
			esUtil = new EsUtil();
		return esUtil;
	}
	
	/**
	 * 获取单例client。不存在时,创建，否则直接返回
	 * @return
	 */
	public Client getClient() {
		try {
			if (this.client == null)
				this.client = createClient();
		} catch (Exception e) {
			logger.error("create client error", e);
		}
		return client;
	}
	
	/**
	 * 功能：创建客户端连接
	 * 
	 * @param type
	 * @return 
	 * @throws UnknownHostException 
	 */
	private Client createClient() throws UnknownHostException {
		logger.debug("prepare to connect elasticsearch servers...");
		Settings settings = Settings.settingsBuilder().put("client.transport.sniff", true)
				.put("cluster.name", Constants.appConfig.getProperty("elasticsearch.cluster.name")).build();
		Client client = TransportClient.builder().settings(settings).build()
				.addTransportAddress(new InetSocketTransportAddress(
						InetAddress.getByName(Constants.appConfig.getProperty("elasticsearch.servers")),
						NumberUtils.toInt(Constants.appConfig.getProperty("elasticsearch.port"))));
		logger.debug("connect elasticsearch servers successfully!");
		return client;
	}
	
	/**
	 * 功能：关闭客户端连接
	 */
	public void closeClient() {
		if (client != null)
			client.close();
	}

	/**
	 * 功能：删除索引文件库
	 * 
	 * @param indexNames
	 * @throws Throwable
	 */
	public void deleteIndex(String indexNames[]) throws Throwable {
		this.createClient();
		try {
			DeleteIndexResponse response = client.admin().indices().prepareDelete(indexNames).execute().actionGet();
			if (response != null && response.isAcknowledged()) {
				logger.debug("delete the indexes successfully");
				return;
			}
			logger.error("delete the indexes error");
		} finally {
			this.closeClient();
		}
	}
	
}
