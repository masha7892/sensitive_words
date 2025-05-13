package com.realtime.util;



import com.stream.common.utils.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisException;

import java.net.URI;
import java.util.*;

/**
 * RedisLuaUtils 类用于通过 Lua 脚本与 Redis 进行交互，支持敏感词的批量检查和单条检查。
 * 使用 Jedis 连接池管理 Redis 连接，并通过预加载 Lua 脚本提高性能。
 */
public class RedisLuaUtils {
    //使用 SLF4J 记录日志，便于调试和监控。
    private static final Logger logger = LoggerFactory.getLogger(RedisLuaUtils.class);
    //设置redis的连接信息
    // Redis 主机地址
    private static final String REDIS_HOST = ConfigUtils.getString("redis.host");
    // Redis 端口
    private static final int REDIS_PORT = ConfigUtils.getInt("redis.port");
    // Redis 用户名
    private static final String REDIS_USER = ConfigUtils.getString("redis.user");
    // Redis 密码
    private static final String REDIS_PASSWORD = ConfigUtils.getString("redis.pwd");
    // Redis 数据库编号
    private static final int REDIS_DB = ConfigUtils.getInt("redis.blacklist.db");
    // Redis 中存储敏感词的集合键名
    private static final String SET_KEY = "sensitive_words";

     /**Lua脚本（支持批量/单条查询）用于批量检查给定成员是否属于某个 Redis 集合
    定义了一个嵌入在字符串中的 Lua 脚本，用于批量检查给定成员是否属于某个 Redis 集合：
    把第一个 KEYS 参数（KEYS[1]）视为要查询的 Redis 集合的键名；
    遍历所有通过 ARGV 传进来的成员值；
    对每个成员调用 SISMEMBER 命令判断其是否在集合中；
    将每个判断结果（1 表示在集合中、0 表示不在）依次存入本地表 results；
    最后把 results 表整体返回给调用者。**/
    private static final String LUA_SCRIPT =
            "local results = {} " +
                    "for i, key in ipairs(ARGV) do " +
                    "    results[i] = redis.call('SISMEMBER', KEYS[1], key) " +
                    "end " +
                    "return results";

    // Lua 脚本的 SHA1 值，用于 EVALSHA 命令
    //EVALSHA 是 Redis 用于执行已缓存 Lua 脚本的命令，
    // 它通过脚本的 SHA1 摘要来引用脚本，从而避免每次都传输完整脚本文本，提高执行效率和节省网络带宽。
    // 若服务端未缓存该脚本，则会返回 NOSCRIPT 错误，需要使用 EVAL 或先调用 SCRIPT LOAD 将脚本加载到缓存中后再执行。
    private static volatile String luaScriptSha;
    // Jedis 连接池
    private static JedisPool jedisPool = null;

    //在类加载时调用 initializeRedisPool()，保证整个应用生命周期内只有一个连接池实例。
    static {
        initializeRedisPool();
    }

    //初始化 Redis 连接池。
    private static void initializeRedisPool() {
        try {
            //配置连接池最大、最小、空闲连接数，并启用借出测试以避免使用无效连接
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(200);// 最大连接数
            poolConfig.setMaxIdle(50);// 最大空闲连接数
            poolConfig.setMinIdle(10);// 最小空闲连接数
            poolConfig.setTestOnBorrow(true);// 借出连接时测试连接是否有效
            //使用 JedisPool 的 URI 构造器，传入完整的 redis://user:pass@host:port/db 格式字符串初始化连接池
            String uri = String.format("redis://%s:%s@%s:%d/%d",
                    REDIS_USER,
                    REDIS_PASSWORD,
                    REDIS_HOST,
                    REDIS_PORT,
                    REDIS_DB);

            jedisPool = new JedisPool(
                    poolConfig,
                    URI.create(uri),
                    Protocol.DEFAULT_TIMEOUT
            );
            // 预加载 Lua 脚本
            preloadLuaScript();
        } catch (Exception e) {
            logger.error("Redis连接初始化失败", e);
            throw new RuntimeException("Redis连接初始化失败", e);
        }
    }



    // Lua 脚本预加载,在连接池可用后首次取连接并执行 scriptLoad，将脚本编译并缓存到 Redis，
    // 返回的 SHA1 存入 luaScriptSha，后续调用可直接使用 EVALSHA 节省网络带宽
    private static void preloadLuaScript() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.ping(); // 测试连接
            luaScriptSha = jedis.scriptLoad(LUA_SCRIPT);
            // 加载脚本并获取 SHA1 值
            logger.info("Lua脚本预加载成功，SHA: {}", luaScriptSha);
        } catch (Exception e) {
            logger.error("Lua脚本预加载失败", e);
            throw new RuntimeException("Lua脚本初始化失败", e);
        }
    }

    // 批量检查
    /**
     * 批量检查多个关键词是否为敏感词
     * keywords 关键词列表
     * 包含每个关键词是否存在（true/false）的 Map
     */
    public static Map<String, Boolean> batchCheck(List<String> keywords) {
        if (keywords == null || keywords.isEmpty()) {
            logger.warn("传入空关键词列表");
            return Collections.emptyMap();
        }

        Map<String, Boolean> result = new HashMap<>(keywords.size());

        try (Jedis jedis = jedisPool.getResource()) {
            ensureScriptLoaded(jedis);// 确保脚本已加载

            // 执行 Lua 脚本
            Object response = executeScript(jedis, keywords);
            // 解析结果
            processResponse(keywords, response, result);
        } catch (Exception e) {
            handleError("批量检查", keywords.size(), e);
        }
        return result;
    }

    // 新增单个检查方法
    /**
     * 检查单个关键词是否存在于敏感词集合中。
     *
     *keyword 关键词
     *关键词是否存在
     */
    public static boolean checkSingle(String keyword) {
        if (keyword == null || keyword.isEmpty()) {
            logger.warn("传入空关键词");
            return false;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            // 确保脚本已加载
            ensureScriptLoaded(jedis);
            // 处理脚本响应
            Object response = executeScript(jedis, Collections.singletonList(keyword));
            return processSingleResponse(response);
        } catch (Exception e) {
            // 处理异常
            handleError("单条检查", 1, e);
            return false;
        }
    }

    //volatile 避免指令重排序，保证其他线程看到正确的 luaScriptSha 值；
    // 双重检查锁定模式保障高并发场景下脚本只加载一次
    private static void ensureScriptLoaded(Jedis jedis) {
        if (luaScriptSha == null) {
            synchronized (RedisLuaUtils.class) {
                if (luaScriptSha == null) {
                    luaScriptSha = jedis.scriptLoad(LUA_SCRIPT);
                    logger.info("运行时重新加载Lua脚本SHA: {}", luaScriptSha);
                }
            }
        }
    }

    //优先使用 EVALSHA 调用缓存脚本；
    // 若 Redis 重启或缓存失效导致返回 NOSCRIPT 错误，则捕获并回退到 EVAL，自动重载脚本再执行
    private static Object executeScript(Jedis jedis, List<String> keywords) {
        try {
            return jedis.evalsha(luaScriptSha,
                    Collections.singletonList(SET_KEY), // KEYS
                    keywords // ARGV
            );
        } catch (JedisException e) {
            if (e.getMessage().contains("NOSCRIPT")) {
                logger.warn("Lua脚本未缓存，重新加载执行");
                return jedis.eval(LUA_SCRIPT,
                        Collections.singletonList(SET_KEY), // KEYS
                        keywords // ARGV
                );
            }
            throw e;
        }
    }

    /**
     * 处理 Lua 脚本的批量响应，将结果映射到关键词列表中。
     *
     * @param keywords 关键词列表
     * @param response 脚本响应
     * @param result   结果映射
     */
    private static void processResponse(List<String> keywords, Object response,
                                        Map<String, Boolean> result) {
        if (response instanceof List) {
            List<Long> rawResults = (List<Long>) response;
            for (int i = 0; i < keywords.size(); i++) {
                result.put(keywords.get(i), rawResults.get(i) == 1L);
            }
        }
    }

    /**
     * 处理 Lua 脚本的单条响应，返回关键词是否存在。
     *
     * @param response 脚本响应
     * @return 关键词是否存在
     */
    private static boolean processSingleResponse(Object response) {
        if (response instanceof List && ((List<?>)response).size() == 1) {
            return ((Long)((List<?>)response).get(0)) == 1L;
        }
        throw new RuntimeException("无效的响应格式");
    }

    //所有脚本调用异常集中通过 handleError 记录日志并抛出统一运行时异常；
    //healthCheck 简单执行 PING，返回 Redis 是否可用。
    private static void handleError(String operationType, int keywordCount, Exception e) {
        String errorMsg = String.format("%s操作失败，关键词数量: %d", operationType, keywordCount);
        logger.error(errorMsg, e);
        throw new RuntimeException("敏感词检查服务暂不可用", e);
    }

    public boolean healthCheck() {
        try (Jedis jedis = jedisPool.getResource()) {
            return "PONG".equals(jedis.ping());
        } catch (Exception e) {
            logger.error("Redis健康检查失败", e);
            return false;
        }
    }

    public static void main(String[] args) {
        // 连接测试
        try (Jedis jedis = jedisPool.getResource()) {
            System.out.println("Redis服务器版本: " + jedis.info("server").split("\n")[0]);
//            System.out.println("当前用户: " + jedis.aclWhoAmI());
            System.out.println("Redis ping 响应: " + jedis.ping());
        } catch (Exception e) {
            logger.error("初始连接测试失败", e);
            return;
        }
        // 测试批量检查
        List<String> testWords = Arrays.asList("真理部", "性爱体位", "胡金淘");
        Map<String, Boolean> batchResults = batchCheck(testWords);
        batchResults.forEach((word, exist) ->
                System.out.printf("[批量] 关键词 [%s] 存在: %s%n", word, exist ? "是" : "否"));

        // 测试单条检查
        testWords.forEach(word -> {
            boolean exists = checkSingle(word);
            System.out.printf("[单条] 关键词 [%s] 存在: %s%n", word, exists ? "是" : "否");
        });

        System.out.println("服务健康状态: " + (new RedisLuaUtils().healthCheck() ? "正常" : "异常"));
    }
}