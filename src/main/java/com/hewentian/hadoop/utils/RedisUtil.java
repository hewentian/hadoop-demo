package com.hewentian.hadoop.utils;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * <p>
 * <b>RedisUtil.java</b> 是 Redis 工具类
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2018-09-15 11:10:21 AM
 * @since JDK 1.8
 */
public class RedisUtil {
    private static Logger log = Logger.getLogger(RedisUtil.class);

    private static String host;
    private static int port;
    private static String passwd;
    private static int index;

    private static JedisPool jedisPool;

    static {
        try {
            host = Config.get("redis.host", "localhost");
            port = Integer.valueOf(Config.get("redis.port", "6379"));
            passwd = Config.get("redis.passwd", "");
            index = Integer.valueOf(Config.get("redis.index", "0"));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static Jedis getJedis() {
        Jedis jedis = new Jedis(host, port);

        // 密码验证，如果你没有设置redis密码可不验证
        jedis.auth(passwd);

//        jedis.select(index); // 也可以在使用的时候再选择哪一个库

        return jedis;
    }

    public static Jedis getJedisFromPool() {
        jedisPool = getJedisPool();
        Jedis jedis = jedisPool.getResource();
        jedis.auth(passwd);

        return jedis;
    }

    private static JedisPool getJedisPool() {
        if (null != jedisPool) {
            return jedisPool;
        }

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(Integer.valueOf(Config.get("redis.maxTotal", "16")));
        jedisPoolConfig.setMinIdle(Integer.valueOf(Config.get("redis.minIdle", "4")));
        jedisPoolConfig.setMaxIdle(Integer.valueOf(Config.get("redis.maxIdle", "16")));
        jedisPoolConfig.setMaxWaitMillis(Long.valueOf(Config.get("redis.maxWaitMillis", "30000")));
        jedisPoolConfig
                .setMinEvictableIdleTimeMillis(Long.valueOf(Config.get("redis.minEvictableIdleTimeMillis", "60000")));
        jedisPoolConfig.setNumTestsPerEvictionRun(Integer.valueOf(Config.get("redis.numTestsPerEvictionRun", "-1")));
        jedisPoolConfig.setSoftMinEvictableIdleTimeMillis(
                Long.valueOf(Config.get("redis.softMinEvictableIdleTimeMillis", "-1")));
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(
                Long.valueOf(Config.get("redis.timeBetweenEvictionRunsMillis", "30000")));
        jedisPoolConfig.setBlockWhenExhausted(Boolean.valueOf(Config.get("redis.blockWhenExhausted", "true")));

        jedisPool = new JedisPool(jedisPoolConfig, host, port);

        return jedisPool;
    }

    public static void close(Jedis jedis) {
        try {
            jedis.disconnect();
            jedis.close();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static void destroyPool() {
        try {
            jedisPool.destroy();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
