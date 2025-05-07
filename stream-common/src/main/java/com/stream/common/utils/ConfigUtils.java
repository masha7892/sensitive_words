package com.stream.common.utils;


import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

//keypoint 配置文件工具类
public class ConfigUtils {
    //在 ConfigUtils 类中创建一个专属的、全局唯一的日志记录器（Logger）
    //传入 ConfigUtils.class 作为参数后，
    //SLF4J 就会使用该类的全限定类名作为日志名称，这样输出的日志就自动带上来源类的信息，便于定位和过滤
    private static final Logger logger = LoggerFactory.getLogger(ConfigUtils.class);

    private static Properties properties;

    static {
        try {
            //使用类加载器,读取配置文件,
            //在pom文件中设置在process-resources阶段
            //common-config.properties配置文件中的mysql.host=${mysql.host}
            //由common-config.properties.dev中的mysql.host=42.51.0.222替换值
            properties = new Properties();
            //ConfigUtils.class：表示这个类对象，可以通过它访问类加载器
            //getClassLoader()：加getClassLoader() 表示从 classpath 根目录查找,不加 getClassLoader() 从当前包路径查找
            //getResourceAsStream("common-config.properties")：表示从classpath 的根目录开始查找这个文件，查到后以 InputStream 的形式返回。
            //Classpath 是 Java 查找类（.class）和资源（如 .properties、.xml）的路径集合
            //在本模块中被当做依赖引入的其他模块,都会被加入本模块的 classpath。
            properties.load(ConfigUtils.class.getClassLoader().getResourceAsStream("common-config.properties"));

        } catch (IOException e) {
            // 1. 记录错误日志
            logger.error("加载配置文件出错, exit 1", e);
            // 2. 强制退出程序
            System.exit(1);
        }
    }


    //输入string返回string类型的配置信息
    public static String getString (String key){
        return properties.getProperty(key).trim();
    }

    //输入string返回int类型的配置信息
    public static int getInt(String key){
        String value = properties.getProperty(key).trim();
        return Integer.parseInt(value);
    }

    //输入string返回int类型的配置信息,如果对应配置信息为null则返回默认值
    public static int getInt(String key,int defaultValue){
        String value = properties.getProperty(key).trim();
        return Strings.isNullOrEmpty(value) ? defaultValue : Integer.parseInt(value);
    }


    //输入string返回long类型的配置信息
    public static long getLong(String key){
        String value = properties.getProperty(key).trim();
        return Long.parseLong(value);
    }

    //输入string返回long类型的配置信息,如果对应配置信息为null则返回默认值
    public static long getLong(String key,long defaultValue){
        String value = properties.getProperty(key).trim();
        return Strings.isNullOrEmpty(value) ? defaultValue : Long.parseLong(value);
    }


}
