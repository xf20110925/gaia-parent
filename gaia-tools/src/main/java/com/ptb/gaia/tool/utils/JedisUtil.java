package com.ptb.gaia.tool.utils;

/**
 * Created by lishuangbin on 15/11/25.
 */

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class JedisUtil {

    private static Logger logger = LoggerFactory.getLogger(JedisUtil.class);
    private static String JEDIS_IP;
    private static int JEDIS_PORT;
    private static String JEDIS_PASSWORD;

    public static final byte[] REDISKEY_URLINSPECT_COMMIT =
            "urlinspectcommit".getBytes();
    public static final byte[] REDISKEY_URLINSPECT_COMMITRESULT =
            "urlinspectcommitresult".getBytes();
    public static final byte[] REDISKEY_URLINSPECT_QUERY =
            "urlinspectquery".getBytes();
    public static final byte[] REDISKEY_URLINSPECT_QUERYRESULT =
            "urlinspectqueryresult".getBytes();

    public static Map<String, Integer> msgId;


    private static JedisPool jedisPool;

    static {

        Configuration config = null;
        try {
            config = new PropertiesConfiguration("ptb.properties");
        } catch (ConfigurationException e) {
            throw new RuntimeException("ptb.properties not found", e);
        }

        // 从配置文件中获取属性值
        JEDIS_IP = config.getString("gaia.service.redis.host", "127.0.0.1");
        JEDIS_PORT = config.getInt("gaia.service.redis.port", 6379);
        JEDIS_PASSWORD = config.getString("gaia.service.redis.password", null);
        JedisPoolConfig jdsConfig = new JedisPoolConfig();
        //config.setMaxActive(5000);


        jdsConfig.setMaxIdle(256);//20
        jdsConfig.setTestOnBorrow(true);
        jdsConfig.setTestOnReturn(true);
        jdsConfig.setTestWhileIdle(true);
        jdsConfig.setMinEvictableIdleTimeMillis(60000l);
        jdsConfig.setTimeBetweenEvictionRunsMillis(3000l);
        jdsConfig.setNumTestsPerEvictionRun(-1);
//        jedisPool = new JedisPool(jdsConfig, JEDIS_IP, JEDIS_PORT, 60000, JEDIS_PASSWORD);
        jedisPool = new JedisPool(jdsConfig, JEDIS_IP, JEDIS_PORT, 60000, JEDIS_PASSWORD,1);

    }

    /**
     * 获取数据
     *
     * @param key
     * @return
     */
    public static String get(String key) {

        String value = null;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            value = jedis.get(key);
        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //返还到连接池
            close(jedis);
        }

        return value;
    }

    public static void close(Jedis jedis) {
        try {
            jedisPool.returnResource(jedis);

        } catch (Exception e) {
            if (jedis.isConnected()) {
                jedis.quit();
                jedis.disconnect();
            }
        }
    }

    /**
     * 获取数据
     *
     * @param key
     * @return
     */
    public static byte[] get(byte[] key) {

        byte[] value = null;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            value = jedis.get(key);
        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //返还到连接池
            close(jedis);
        }

        return value;
    }

    public static void set(byte[] key, byte[] value) {

        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.set(key, value);
        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //返还到连接池
            close(jedis);
        }
    }

    public static void set(byte[] key, byte[] value, int time) {

        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.set(key, value);
            jedis.expire(key, time);
        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //返还到连接池
            close(jedis);
        }
    }

    public static void hset(byte[] key, byte[] field, byte[] value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.hset(key, field, value);
        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //返还到连接池
            close(jedis);
        }
    }

    public static void hset(String key, String field, String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.hset(key, field, value);
        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //返还到连接池
            close(jedis);
        }
    }

    /**
     * 获取数据
     *
     * @param key
     * @return
     */
    public static String hget(String key, String field) {

        String value = null;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            value = jedis.hget(key, field);
        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //返还到连接池
            close(jedis);
        }

        return value;
    }

    /**
     * 获取数据
     *
     * @param key
     * @return
     */
    public static byte[] hget(byte[] key, byte[] field) {

        byte[] value = null;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            value = jedis.hget(key, field);
        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //返还到连接池
            close(jedis);
        }

        return value;
    }

    public static void hdel(byte[] key, byte[] field) {

        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.hdel(key, field);
        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //返还到连接池
            close(jedis);
        }
    }

    /**
     * 存储REDIS队列 顺序存储
     *
     * @param value key reids键名
     * @param key   value 键值
     */
    public static void lpush(byte[] key, byte[] value) {

        Jedis jedis = null;
        try {

            jedis = jedisPool.getResource();
            jedis.lpush(key, value);

        } catch (Exception e) {

            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();

        } finally {

            //返还到连接池
            close(jedis);

        }
    }

    /**
     * 存储REDIS队列 反向存储
     *
     * @param value key reids键名
     * @param value value 键值
     */
    public static void rpush(byte[] key, byte[] value) {

        Jedis jedis = null;
        try {

            jedis = jedisPool.getResource();
            jedis.rpush(key, value);

        } catch (Exception e) {

            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();

        } finally {

            //返还到连接池
            close(jedis);

        }
    }

    /**
     * 将列表 source 中的最后一个元素(尾元素)弹出，并返回给客户端
     *
     * @param destination key reids键名
     * @param destination value 键值
     */
    public static void rpoplpush(byte[] key, byte[] destination) {

        Jedis jedis = null;
        try {

            jedis = jedisPool.getResource();
            jedis.rpoplpush(key, destination);

        } catch (Exception e) {

            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();

        } finally {

            //返还到连接池
            close(jedis);

        }
    }

    /**
     * 获取队列数据
     *
     * @return
     */
    public static List<byte[]> lpopList(byte[] key) {

        List<byte[]> list = null;
        Jedis jedis = null;
        try {

            jedis = jedisPool.getResource();
            list = jedis.lrange(key, 0, -1);

        } catch (Exception e) {

            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();

        } finally {

            //返还到连接池
            close(jedis);

        }
        return list;
    }

    /**
     * 获取队列数据
     *
     * @param key key 键名
     * @return
     */
    public static byte[] rpop(byte[] key) {

        byte[] bytes = null;
        Jedis jedis = null;
        try {

            jedis = jedisPool.getResource();
            bytes = jedis.rpop(key);

        } catch (Exception e) {

            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();

        } finally {

            //返还到连接池
            close(jedis);

        }
        return bytes;
    }

    public static void hmset(Object key, Map<String, String> hash) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.hmset(key.toString(), hash);
        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();

        } finally {
            //返还到连接池
            close(jedis);

        }
    }

    public static void hmset(Object key, Map<String, String> hash, int time) {
        Jedis jedis = null;
        try {

            jedis = jedisPool.getResource();
            jedis.hmset(key.toString(), hash);
            jedis.expire(key.toString(), time);
        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();

        } finally {
            //返还到连接池
            close(jedis);

        }
    }

    public static List<String> hmget(Object key, String... fields) {
        List<String> result = null;
        Jedis jedis = null;
        try {

            jedis = jedisPool.getResource();
            result = jedis.hmget(key.toString(), fields);

        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();

        } finally {
            //返还到连接池
            close(jedis);

        }
        return result;
    }

    public static Set<String> hkeys(String key) {
        Set<String> result = null;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            result = jedis.hkeys(key);

        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();

        } finally {
            //返还到连接池
            close(jedis);

        }
        return result;
    }

    public static List<byte[]> lrange(byte[] key, int from, int to) {
        List<byte[]> result = null;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            result = jedis.lrange(key, from, to);

        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();

        } finally {
            //返还到连接池
            close(jedis);

        }
        return result;
    }

    public static Map<byte[], byte[]> hgetAll(byte[] key) {
        Map<byte[], byte[]> result = null;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            result = jedis.hgetAll(key);
        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();

        } finally {
            //返还到连接池
            close(jedis);
        }
        return result;
    }

    public static void del(byte[] key) {

        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.del(key);
        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //返还到连接池
            close(jedis);
        }
    }

    public static long llen(byte[] key) {

        long len = 0;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.llen(key);
        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //返还到连接池
            close(jedis);
        }
        return len;
    }


    public static void deleteAllKey(String lamda) {

        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Set<String> keys = jedis.keys(lamda);
            for (String key : keys) {
                jedis.del(key);
            }
        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            logger.error("delete all key error! ip:{} port:{}", JEDIS_IP, JEDIS_PORT, e);
        } finally {
            //返还到连接池
            close(jedis);
        }

    }


    public static void flushAll() {


        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.flushAll();
        } catch (Exception e) {
            //释放redis对象
            jedisPool.returnBrokenResource(jedis);
            logger.error("connect to redis error! ip:{} port:{}", JEDIS_IP, JEDIS_PORT, e);
        } finally {
            //返还到连接池
            close(jedis);
        }
    }

    public static void zadd(String key, Map<String, Double> scoreMembers, int timeOut) {
        Jedis jedis = null;
        if (scoreMembers.size() <= 0) {
            return;
        }
        try {
            jedis = jedisPool.getResource();
            jedis.zadd(key, scoreMembers);
            jedis.expire(key, timeOut);
        } catch (Exception e) {
            jedisPool.returnBrokenResource(jedis);
            logger.error("connect to redis error! ip:{} port:{}", JEDIS_IP, JEDIS_PORT, e);
        } finally {
            close(jedis);
        }
    }

    public static String getMD5Key(String key) {
        return DigestUtils.md5Hex(key);
    }

    public static Set<String> zget(String key, int start, int end) {
        Jedis jedis = null;
        if(start > end)  {
            return new HashSet<>();
        }
        try {
            jedis = jedisPool.getResource();
            return jedis.zrevrange(key, start, end);
        } catch (Exception e) {
            jedisPool.returnBrokenResource(jedis);
            logger.error("connect to redis error! ip:{} port:{}", JEDIS_IP, JEDIS_PORT, e);
        } finally {
            close(jedis);
        }
        return null;
    }

    public static long zKeySize(String key){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return jedis.zcard(key);
        } catch (Exception e) {
            jedisPool.returnBrokenResource(jedis);
            logger.error("connect to redis error! ip:{} port:{}", JEDIS_IP, JEDIS_PORT, e);
        } finally {
            close(jedis);
        }
        return 0;
    }
}

