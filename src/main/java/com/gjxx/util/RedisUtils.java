package com.gjxx.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author Sxs
 * @description redis工具类
 * @date 2019/10/10 17:09
 */
public class RedisUtils {

    /**
     * 服务器IP地址
     */
    private static final String ADDR = "192.168.1.110";

    /**
     * 端口
     */
    private static final int PORT = 6379;

    /**
     * 密码
     */
    private static final String AUTH = "123456";

    /**
     * 连接实例的最大连接数
     */
    private static final int MAX_ACTIVE = 1024;

    /**
     * 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
     */
    private static final int MAX_IDLE = 200;

    /**
     * 等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException
     */
    private static final int MAX_WAIT = 10000;

    /**
     * 连接超时的时间　
     */
    private static final int TIMEOUT = 10000;

    /**
     * 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
     */
    private static final boolean TEST_ON_BORROW = true;

    private static JedisPool jedisPool = null;

    /**
     * 数据库模式是16个数据库 0~15
     */
    private static final int DEFAULT_DATABASE = 0;

    // 初始化Redis连接池
    static {

        try {

            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(MAX_ACTIVE);
            config.setMaxIdle(MAX_IDLE);
            config.setMaxWaitMillis(MAX_WAIT);
            config.setTestOnBorrow(TEST_ON_BORROW);
            jedisPool = new JedisPool(config, ADDR, PORT, TIMEOUT, AUTH, DEFAULT_DATABASE);

        } catch (Exception e) {

            e.printStackTrace();
        }

    }

    /**
     * 获取Jedis实例
     * @return Jedis
     */
    public synchronized static Jedis getJedis() {
        try {
            if (jedisPool != null) {
                Jedis resource = jedisPool.getResource();
                System.out.println("redis--服务正在运行: "+resource.ping());
                return resource;
            } else {
                return null;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    /**
     * 释放资源
     * @param jedis 要释放的连接
     */
    public static void returnResource(final Jedis jedis) {
        if(jedis != null) {
            jedis.close();
        }

    }

}
