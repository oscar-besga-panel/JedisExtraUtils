package org.oba.jedis.extra.utils.interruptinglocks.functional;

import org.oba.jedis.extra.utils.interruptinglocks.IJedisLock;
import org.oba.jedis.extra.utils.interruptinglocks.JedisLock;
import org.oba.jedis.extra.utils.interruptinglocks.LockFromRedis;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.oba.jedis.extra.utils.utils.JedisPoolAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

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
        if (lock instanceof LockFromRedis) {
            LockFromRedis jedisLockFromRedis = (LockFromRedis) lock;
            LOGGER.info("interruptingLock.isLocked() " + jedisLockFromRedis.isLocked() + " for thread " + Thread.currentThread().getName());
            if (jedisLockFromRedis.isLocked()) {
                LOGGER.debug("LOCKED");
                return true;
            } else {
                IllegalStateException ise =  new IllegalStateException("LOCK NOT ADQUIRED isLocked " + jedisLockFromRedis.isLocked());
                LOGGER.error("ERROR LOCK NOT ADQUIRED e {} ", ise.getMessage(), ise);
                throw ise;
            }
        } else {
            return true;
        }
    }


    /**
     * Main method
     * Run only if avalible connnection on jedis.test.properties file
     * @param args arguments
     */
    public static void main(String[] args) {
        LOGGER.debug("main ini >>>> ");
        JedisTestFactory jtfTest = JedisTestFactory.get();
        Jedis jedis = jtfTest.createJedisClient();
        jtfTest.testConnection();
        JedisPoolAdapter jedisPoolAdapter = JedisPoolAdapter.poolFromJedis(jedis);
        LockFromRedis jedisLockFromRedis = new JedisLock(jedisPoolAdapter,"jedisLockSc").asConcurrentLock();
        boolean locked = jedisLockFromRedis.tryLock();
        boolean reallyLocked = jedisLockFromRedis.isLocked();
        jedisLockFromRedis.unlock();
        jedisPoolAdapter.close();
        jedis.close();
        System.out.println("JEDISLOCK " + locked + " " + reallyLocked);

        jtfTest.testPoolConnection();
        JedisPool jedisPool = jtfTest.createJedisPool();
        LockFromRedis jedisPoolLock = new JedisLock(jedisPool,"jedisPoolLock").asConcurrentLock();
        boolean plocked = jedisPoolLock.tryLock();
        boolean preallyLocked = jedisPoolLock.isLocked();
        jedisPoolLock.unlock();
        jedisPool.close();
        System.out.println("JEDISPOOLLOCK " + plocked + " " + preallyLocked);
        LOGGER.debug("main fin <<<< ");

    }


}
