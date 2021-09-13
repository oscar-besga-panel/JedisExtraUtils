package org.obapanel.jedis.interruptinglocks.functional;

import org.obapanel.jedis.common.test.JedisTestFactory;
import org.obapanel.jedis.interruptinglocks.IJedisLock;
import org.obapanel.jedis.interruptinglocks.JedisLock;
import org.obapanel.jedis.interruptinglocks.Lock;
import org.obapanel.jedis.utils.JedisPoolAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

public class JedisTestFactoryLocks {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisTestFactoryLocks.class);

    static boolean checkLock(IJedisLock jedisLock){
        LOGGER.info("interruptingLock.isLocked() " + jedisLock.isLocked() + " for thread " + Thread.currentThread().getName());
        if (jedisLock.isLocked()) {
            LOGGER.info("LOCKED " +  Thread.currentThread().getName());
            return true;
        } else {
            IllegalStateException ise =  new IllegalStateException("LOCK NOT ADQUIRED isLocked " + jedisLock.isLocked() + " for thread " + Thread.currentThread().getName());
            LOGGER.error("ERROR LOCK NOT ADQUIRED for thread {} e {} ", Thread.currentThread().getName(),  ise.getMessage(), ise);
            throw ise;
        }
    }

    static boolean checkLock(java.util.concurrent.locks.Lock lock){
        if (lock instanceof  org.obapanel.jedis.interruptinglocks.Lock) {
            org.obapanel.jedis.interruptinglocks.Lock jedisLock = (org.obapanel.jedis.interruptinglocks.Lock) lock;
            LOGGER.info("interruptingLock.isLocked() " + jedisLock.isLocked() + " for thread " + Thread.currentThread().getName());
            if (jedisLock.isLocked()) {
                LOGGER.debug("LOCKED");
                return true;
            } else {
                IllegalStateException ise =  new IllegalStateException("LOCK NOT ADQUIRED isLocked " + jedisLock.isLocked());
                LOGGER.error("ERROR LOCK NOT ADQUIRED e {} ", ise.getMessage(), ise);
                throw ise;
            }
        } else {
            return true;
        }
    }



    public static void main(String[] args) {
        JedisTestFactory jtfTest = JedisTestFactory.get();
        Jedis jedis = jtfTest.createJedisClient();
        jtfTest.testConnection();
        JedisPoolAdapter jedisPoolAdapter = JedisPoolAdapter.poolFromJedis(jedis);
        Lock jedisLock = new JedisLock(jedisPoolAdapter,"jedisLockSc").asConcurrentLock();
        boolean locked = jedisLock.tryLock();
        boolean reallyLocked = jedisLock.isLocked();
        jedisLock.unlock();
        jedisPoolAdapter.close();
        jedis.close();
        System.out.println("JEDISLOCK " + locked + " " + reallyLocked);

        jtfTest.testPoolConnection();
        JedisPool jedisPool = jtfTest.createJedisPool();
        Lock jedisPoolLock = new JedisLock(jedisPool,"jedisPoolLock").asConcurrentLock();
        boolean plocked = jedisPoolLock.tryLock();
        boolean preallyLocked = jedisPoolLock.isLocked();
        jedisPoolLock.unlock();
        jedisPool.close();
        System.out.println("JEDISPOOLLOCK " + plocked + " " + preallyLocked);

    }


}
