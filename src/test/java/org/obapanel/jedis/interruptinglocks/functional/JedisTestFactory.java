package org.obapanel.jedis.interruptinglocks.functional;

import org.obapanel.jedis.interruptinglocks.IJedisLock;
import org.obapanel.jedis.interruptinglocks.JedisLock;
import org.obapanel.jedis.interruptinglocks.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.SetParams;

import java.time.Duration;

public class JedisTestFactory {

    private static final Logger log = LoggerFactory.getLogger(JedisTestFactory.class);

    // Zero to prevent any functional test
    // One to one pass
    // More to more passes
    public static final int FUNCTIONAL_TEST_CYCLES = 25;

    public static final String HOST = "127.0.0.1";
    public static final int PORT = 6739;
    public static final String PASS = "";

    public static final String URI = "redis://" + HOST + ":" + PORT;


    static boolean functionalTestEnabled(){
        return FUNCTIONAL_TEST_CYCLES > 0;
    }

    static Jedis createJedisClient(){
        HostAndPort hostAndPort = new HostAndPort(HOST,PORT);
        Jedis jedis = new Jedis(hostAndPort);
        return authJedis(jedis);
    }

    static JedisPool createJedisPool(){
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
        JedisPool jedisPool = new JedisPool(jedisPoolConfig, HOST, PORT);
        /*
        if (PASS != null && !   PASS.trim().isEmpty()) {
            jedisPool.auth(PASS);
        }
        */

        return jedisPool;
    }

    static Jedis authJedis(Jedis jedis){
        if (PASS != null && !PASS.trim().isEmpty()) {
            jedis.auth(PASS);
        }
        return jedis;
    }

    public static Jedis testConnection(Jedis jedis){
        String val = "test:" + System.currentTimeMillis();
        jedis.set(val,val,new SetParams().px(5000));
        String check = jedis.get(val);
        jedis.del(val);
        if (!val.equalsIgnoreCase(check)) throw new IllegalStateException("Jedis connection not ok");
        return jedis;
    }


    static boolean checkLock(IJedisLock jedisLock){
        log.info("interruptingLock.isLocked() " + jedisLock.isLocked() + " for thread " + Thread.currentThread().getName());
        if (jedisLock.isLocked()) {
            log.debug("LOCKED");
            return true;
        } else {
            IllegalStateException ise =  new IllegalStateException("LOCK NOT ADQUIRED isLocked " + jedisLock.isLocked());
            log.error("ERROR LOCK NOT ADQUIRED e {} ", ise.getMessage(), ise);
            throw ise;
        }
    }

    static boolean checkLock(java.util.concurrent.locks.Lock lock){
        if (lock instanceof  org.obapanel.jedis.interruptinglocks.Lock) {
            org.obapanel.jedis.interruptinglocks.Lock jedisLock = (org.obapanel.jedis.interruptinglocks.Lock) lock;
            log.info("interruptingLock.isLocked() " + jedisLock.isLocked() + " for thread " + Thread.currentThread().getName());
            if (jedisLock.isLocked()) {
                log.debug("LOCKED");
                return true;
            } else {
                IllegalStateException ise =  new IllegalStateException("LOCK NOT ADQUIRED isLocked " + jedisLock.isLocked());
                log.error("ERROR LOCK NOT ADQUIRED e {} ", ise.getMessage(), ise);
                throw ise;
            }
        } else {
            return true;
        }
    }



    public static void main(String[] args) {
        Jedis jedis = JedisTestFactory.createJedisClient();
        testConnection(jedis);
        Lock jedisLock = new JedisLock(jedis,"jedisLock").asConcurrentLock();
        boolean locked = jedisLock.tryLock();
        jedisLock.unlock();
        jedis.quit();
        System.out.println("JEDISLOCK");

    }

}
