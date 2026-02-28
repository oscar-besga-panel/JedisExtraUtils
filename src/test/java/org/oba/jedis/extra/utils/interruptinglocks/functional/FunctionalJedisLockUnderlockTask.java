package org.oba.jedis.extra.utils.interruptinglocks.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.interruptinglocks.InterruptingJedisJedisLockBase;
import org.oba.jedis.extra.utils.interruptinglocks.JedisLock;
import org.oba.jedis.extra.utils.interruptinglocks.JedisLockUtils;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import redis.clients.jedis.UnifiedJedis;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.oba.jedis.extra.utils.iterators.ScanUtil.deleteListOfKeysStartsWith;

public class FunctionalJedisLockUnderlockTask {

    private static final String COMMON_REDIS_TEST_NAME = "lock:" + FunctionalJedisLockUnderlockTask.class.getName() + ":";

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private UnifiedJedis redisClient;
    private String keyName;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        redisClient = jtfTest.createRedisClient();
        keyName = COMMON_REDIS_TEST_NAME + System.currentTimeMillis();
    }


    @After
    public void tearDown() {
        if (!jtfTest.functionalTestEnabled()) return;
        if (redisClient != null) {
            redisClient.del(keyName);
            deleteListOfKeysStartsWith(redisClient, COMMON_REDIS_TEST_NAME);
            redisClient.close();
        }
    }


    @Test(timeout = 35000)
    public void underLockTask() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        JedisLockUtils.underLockTask(redisClient, keyName, () -> result1.set(true) );
        boolean result2 = JedisLockUtils.underLockTask(redisClient, keyName, () -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test(timeout = 35000)
    public void underLockTaskSc() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        JedisLockUtils.underLockTask(redisClient, keyName, () -> result1.set(true) );
        AtomicBoolean result2 = new AtomicBoolean(false);
        boolean result2tmp = JedisLockUtils.underLockTask(redisClient, keyName, () -> true);
        result2.set(result2tmp);
        assertTrue(result1.get());
        assertTrue(result2.get());
    }

    @Test(timeout = 35000)
    public void underLock() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        JedisLock jedisLock1 = new JedisLock(redisClient, keyName);
        jedisLock1.underLock(() -> result1.set(true) );
        JedisLock jedisLock2 = new JedisLock(redisClient, keyName);
        boolean result2 = jedisLock2.underLock(() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test(timeout = 35000)
    public void underLockWithInterrupted() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        InterruptingJedisJedisLockBase jedisLock1 = new InterruptingJedisJedisLockBase(redisClient, keyName, 1, TimeUnit.SECONDS);
        jedisLock1.underLock(() -> result1.set(true));
        InterruptingJedisJedisLockBase jedisLock2 = new InterruptingJedisJedisLockBase(redisClient, keyName, 1, TimeUnit.SECONDS);
        boolean result2 = jedisLock2.underLock(() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

}
