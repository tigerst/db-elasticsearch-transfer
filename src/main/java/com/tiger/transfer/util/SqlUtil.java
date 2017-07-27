package com.tiger.transfer.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiger.transfer.Task.Db2EsByTimeTask;
import com.tiger.transfer.callback.Oracle2EsByTimeCallbackImpl;

public class SqlUtil {

	private static final Logger logger = LoggerFactory.getLogger(Oracle2EsByTimeCallbackImpl.class);
	
	/**
	 * 获取sql,缓存存在则都缓存，否则读取文件
	 * @return
	 * @throws IOException
	 * @throws UnsupportedEncodingException
	 */
	public static String getSql() {
		try {
			String sql = Constants.appConfig.getProperty("elasticsearch.db.transfersql");
			if(StringUtils.isNotEmpty(sql))	//缓存中存在，则从缓存读取
				return sql;
			String fileName = Constants.appConfig.getProperty("elasticsearch.db.sql.path");
			InputStream is = null;
			if (fileName.startsWith(Constants.OUTER_FILE_PREFIX))
				is = Files.newInputStream(Paths.get(fileName.substring(Constants.OUTER_FILE_PREFIX.length()))); // 加载外部文件配置
			else
				is = Db2EsByTimeTask.class.getClassLoader().getResourceAsStream(fileName); // 加载项目中配置文件输入流
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			int i = -1;
			while ((i = is.read()) != -1)
				baos.write(i);
			sql = baos.toString("UTF-8");
			Constants.appConfig.setProperty("elasticsearch.db.transfersql", sql);	//加入缓存
			return sql;
		} catch (Exception e) {
			logger.error("get sql error", e);
		}
		return null;
	}
	
	public static List<String> getFields(ResultSet rs){
		List<String> fields = null;
		try {
			ResultSetMetaData metaData = rs.getMetaData();
			fields = new ArrayList<String>();
			int columnCount = metaData.getColumnCount();
			String columnName = null;
			for (int j = 1; j <= columnCount; j++) {
				columnName = metaData.getColumnName(j);
				if (StringUtils.equalsAnyIgnoreCase(columnName, "RN")) // 排除RN-rownum,RN为oracle分页查询的ROWNUM, mysql使用limit
					continue;
				fields.add(columnName); // 存储字段名
			}
		} catch (Exception e) {
			logger.error("get fields error", e);
		}
		return fields;
	}
	
}
