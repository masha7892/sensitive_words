package com.realtime.func;

import com.realtime.util.SensitiveWordsUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.*;

//读取文件数据,写入Redis
public class FileToRedis {
    private static JedisPool jedisPool;
    static {
        // 初始化连接池
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        jedisPool = new JedisPool(poolConfig, "cdh03", 6379, 2000, "123456"); // 替换为你的 Redis 密码
    }

    /**
     * 将文件中的关键词逐行读取并写入 Redis 的 sensitive_words 集合。
     */
    public static void loadWordsToRedis() {

        // 连接 Redis
        try (Jedis jedis = jedisPool.getResource(); // 从连接池获取 Jedis 实例
             //读取文件
             InputStream is = SensitiveWordsUtils.class.getClassLoader().getResourceAsStream("Identify-sensitive-words.txt");
             BufferedReader reader = new BufferedReader(new InputStreamReader(is,"utf-8"))) {


            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) { // 忽略空行
                    //用于向 Redis 的集合（Set）中添加一个或多个成员。
                    jedis.sadd("sensitive_words", line.trim()); // 将关键词添加到集合
                }
            }
            System.out.println("关键词已成功写入 Redis");
        } catch (IOException e) {
            System.err.println("文件读取失败: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        loadWordsToRedis();
    }
}
