package org.obapanel.jedis.countdownlatch.functional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.params.SetParams;

import java.time.Duration;

public class JedisTestFactory {

    private static final Logger log = LoggerFactory.getLogger(JedisTestFactory.class);

    // Zero to prevent any functional test
    // One to one pass
    // More to more passes
    static final int FUNCTIONAL_TEST_CYCLES = 1;

    public static final String HOST = "127.0.0.1";
    public static final int PORT = 32770;
    public static final String PASS = "";


    public static final String URI = "redis://" + HOST + ":" + PORT;


    static boolean functionalTestEnabled(){
        return FUNCTIONAL_TEST_CYCLES > 0;
    }

    static Jedis createJedisClient(){
        HostAndPort hostAndPort = new HostAndPort(HOST,PORT);
        Jedis jedis = new Jedis(hostAndPort);
        if (PASS != null && !PASS.trim().isEmpty()) {
            jedis.auth(PASS);
        }
        return jedis;
    }

    static JedisPool createJedisPool() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(128);
        jedisPoolConfig.setMaxIdle(128);
        jedisPoolConfig.setMinIdle(16);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnReturn(true);
        jedisPoolConfig.setTestWhileIdle(true);
        jedisPoolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
        jedisPoolConfig.setNumTestsPerEvictionRun(3);
        jedisPoolConfig.setBlockWhenExhausted(true);
        if (PASS != null && !PASS.trim().isEmpty()) {
            return new JedisPool(jedisPoolConfig, HOST, PORT, Protocol.DEFAULT_TIMEOUT, PASS);
        } else {
            return new JedisPool(jedisPoolConfig, HOST, PORT);
        }
    }

    public static Jedis testConnection(Jedis jedis){
        String val = "test:" + System.currentTimeMillis();
        jedis.set(val,val,new SetParams().px(5000));
        String check = jedis.get(val);
        jedis.del(val);
        if (!val.equalsIgnoreCase(check)) throw new IllegalStateException("Jedis connection not ok");
        return jedis;
    }

    public static JedisPool testPoolConnection(JedisPool jedisPool){
        Jedis jedis = jedisPool.getResource();
        String val = "test:" + System.currentTimeMillis();
        jedis.set(val,val,new SetParams().px(5000));
        String check = jedis.get(val);
        jedis.del(val);
        if (!val.equalsIgnoreCase(check)) throw new IllegalStateException("Jedis connection not ok");
        jedis.close();
        return jedisPool;
    }





    public static void main(String[] args) {
        Jedis jedis = JedisTestFactory.createJedisClient();
        testConnection(jedis);

        JedisPool jedisPool = JedisTestFactory.createJedisPool();
        testPoolConnection(jedisPool);

    }


}
