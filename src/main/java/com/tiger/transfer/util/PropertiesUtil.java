package com.tiger.transfer.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.lang3.StringUtils;

public class PropertiesUtil {

	/**
	 * 加载配置文件
	 * @param args
	 * @throws IOException
	 */
	public static void loadProperties(String configPath) throws IOException {
		String fileName = "application.properties";
		if (StringUtils.isNotEmpty(configPath))
			fileName = configPath;
		InputStream is = null;
		if (fileName.startsWith(Constants.OUTER_FILE_PREFIX))
			is = Files.newInputStream(Paths.get(fileName.substring(Constants.OUTER_FILE_PREFIX.length()))); // 加载外部文件配置
		else
			is = PropertiesUtil.class.getClassLoader().getResourceAsStream(fileName); // 加载项目中配置文件输入流
		Constants.appConfig.load(is);// 将输入流中的配置加载进Properties
	}
	
}
