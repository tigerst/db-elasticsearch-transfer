package com.tiger.transfer.util;

import java.util.Properties;

public class Constants {
	
    public static final String DEFAULT_ENCODING = "UTF-8";	//默认编码
    
    public static final String PATH_SPLITER = "/";	//路径分隔符
    
    public static final String PUBLIC_KEY = "RSAPublicKey"; // 获取公钥的key
    
    public static final String PRIVATE_KEY = "RSAPrivateKey"; // 获取私钥的key
    
    public static Properties appConfig = new Properties();//公钥私钥密钥
    
    public static final String OUTER_FILE_PREFIX = "file://";	//外部文件头标记
    
}
