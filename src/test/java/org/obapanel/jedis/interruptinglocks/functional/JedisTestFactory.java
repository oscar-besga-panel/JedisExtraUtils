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

public class JedisTestFactory {

    private static final Logger log = LoggerFactory.getLogger(JedisTestFactory.class);

    // Zero to prevent any functional test
    // One to one pass
    // More to more passes
    public static final int TEST_CYCLES = 25;

    public static final String HOST = "127.0.0.1";
    public static final int PORT = 6379;
    public static final String URI = "redis://" + HOST + ":" + PORT;

    static boolean functionalTestEnabled(){
        return TEST_CYCLES > 0;
    }

    static Jedis createJedisClient(){
        HostAndPort hostAndPort = new HostAndPort(HOST,PORT);
        Jedis jedis = new Jedis(hostAndPort);

        return jedis;
    }

    static JedisPool createJedisPool(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMinIdle(0);
        jedisPoolConfig.setMaxIdle(0);
        jedisPoolConfig.setTestOnCreate(true);
        jedisPoolConfig.setTestOnReturn(true);
        jedisPoolConfig.setTestOnBorrow(true);
        JedisPool jedisPool = new JedisPool(jedisPoolConfig, HOST, PORT);
        return jedisPool;
    }


    static boolean checkLock(IJedisLock jedisLock){
        System.out.println("interruptingLock.isLocked() " + jedisLock.isLocked() + " for thread " + Thread.currentThread().getName());
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
            System.out.println("interruptingLock.isLocked() " + jedisLock.isLocked() + " for thread " + Thread.currentThread().getName());
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
        jedis.set("x",System.currentTimeMillis()+"");
        Lock jedisLock = new JedisLock(jedis,"jedisLock").asConcurrentLock();
        boolean locked = jedisLock.tryLock();
        jedisLock.unlock();
        jedis.quit();
        System.out.println("JEDISLOCK");

    }

}
