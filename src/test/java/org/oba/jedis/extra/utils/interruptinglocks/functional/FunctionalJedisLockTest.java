package org.oba.jedis.extra.utils.interruptinglocks.functional;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.interruptinglocks.JedisLock;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import redis.clients.jedis.UnifiedJedis;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.oba.jedis.extra.utils.iterators.ScanUtil.deleteListOfKeysStartsWith;
import static org.junit.Assert.*;

public class FunctionalJedisLockTest {

    private static final String COMMON_REDIS_TEST_NAME = "lock:" + FunctionalJedisLockTest.class.getName() + ":";

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private UnifiedJedis redisClient;
    private String keyName;

    @Before
    public void setup() {
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
    public void testLock() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        if (!jtfTest.functionalTestEnabled()) return;
        JedisLock jedisLock = new JedisLock(redisClient, keyName);
        jedisLock.lock();
        assertTrue(jedisLock.isLocked());
        Assert.assertEquals(getJedisLockUniqueToken(jedisLock), redisClient.get(jedisLock.getName()));
        jedisLock.unlock();
        assertFalse(jedisLock.isLocked());
        assertNull(redisClient.get(jedisLock.getName()));
    }

    @Test(timeout = 35000)
    public void testTryLock() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        if (!jtfTest.functionalTestEnabled()) return;
        JedisLock jedisLock1 = new JedisLock(redisClient, keyName);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        Assert.assertEquals(getJedisLockUniqueToken(jedisLock1), redisClient.get(jedisLock1.getName()));
        JedisLock jedisLock2 = new JedisLock(redisClient, keyName);
        boolean result2 = jedisLock2.tryLock();
        assertFalse(jedisLock2.isLocked());
        assertFalse(result2);
        Assert.assertNotEquals(getJedisLockUniqueToken(jedisLock2), redisClient.get(jedisLock2.getName()));
        jedisLock1.unlock();
    }

    @Test(timeout = 35000)
    public void testTryLockForAWhile() throws InterruptedException {
        if (!jtfTest.functionalTestEnabled()) return;
        JedisLock jedisLock1 = new JedisLock(redisClient, keyName);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        JedisLock jedisLock2 = new JedisLock(redisClient, keyName);
        boolean result2 = jedisLock2.tryLockForAWhile(1, TimeUnit.SECONDS);
        assertFalse(jedisLock2.isLocked());
        assertFalse(result2);
        jedisLock1.unlock();
    }

    @Test(timeout = 35000)
    public void testLockInterruptibly() throws InterruptedException {
        if (!jtfTest.functionalTestEnabled()) return;
        JedisLock jedisLock1 = new JedisLock(redisClient, keyName);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        JedisLock jedisLock2 = new JedisLock(redisClient, keyName);
        final AtomicBoolean triedLock = new AtomicBoolean(false);
        final AtomicBoolean interrupted = new AtomicBoolean(false);
        Thread t = new Thread(() -> {
            try {
                triedLock.set(true);
                jedisLock2.lockInterruptibly();
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        });
        t.setDaemon(true);
        t.start();
        Thread.sleep(1000);
        t.interrupt();
        Thread.sleep(25);

        assertFalse(jedisLock2.isLocked());
        assertTrue(triedLock.get());
        assertTrue(interrupted.get());
        jedisLock1.unlock();
    }


    @Test(timeout = 35000)
    public void testLockNotInterruptibly() throws InterruptedException {
        if (!jtfTest.functionalTestEnabled()) return;
        JedisLock jedisLock1 = new JedisLock(redisClient, keyName);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        JedisLock jedisLock2 = new JedisLock(redisClient, keyName);
        final AtomicBoolean triedLock = new AtomicBoolean(false);
        final AtomicBoolean interrupted = new AtomicBoolean(false);
        Thread t = new Thread(() -> {
            try {
                triedLock.set(true);
                jedisLock2.lock();
            } catch (Exception e) {
                interrupted.set(true);
            }
        });
        t.setDaemon(true);
        t.start();
        Thread.sleep(1000);
        t.interrupt();
        Thread.sleep(25);

        //assertFalse(jedisLock2.isLocked());
        assertTrue(triedLock.get());
        assertFalse(interrupted.get());
        jedisLock1.unlock();
        Thread.sleep(25);
        jedisLock2.unlock();

    }

    @Test(timeout = 35000)
    public void testOneLockWithLeaseTime() throws InterruptedException {
        if (!jtfTest.functionalTestEnabled()) return;
        JedisLock jedisLock1 = new JedisLock(redisClient,  keyName, 5L, TimeUnit.SECONDS);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(result1);
        assertTrue(jedisLock1.isLocked());
        Thread.sleep(5500);
        assertFalse(jedisLock1.isLocked());
    }

    @Test(timeout = 35000)
    public void testLocksWithLeaseTime() throws InterruptedException {
        if (!jtfTest.functionalTestEnabled()) return;
        JedisLock jedisLock1 = new JedisLock(redisClient, keyName,5L, TimeUnit.SECONDS);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        JedisLock jedisLock2 = new JedisLock(redisClient, keyName);
        boolean result2 = jedisLock2.tryLockForAWhile(1, TimeUnit.SECONDS);
        assertFalse(jedisLock2.isLocked());
        assertFalse(result2);
        Thread.sleep(5000);
        JedisLock jedisLock3 = new JedisLock(redisClient, keyName);
        boolean result3 = jedisLock3.tryLockForAWhile(1, TimeUnit.SECONDS);
        assertTrue(jedisLock3.isLocked());
        assertTrue(result3);
        assertFalse(jedisLock1.isLocked());
        jedisLock1.unlock();
        jedisLock3.unlock();
    }

    @Test(timeout = 35000)
    public void testLockWithUpdatedTime() throws InterruptedException {
        try (Jedis jedis = jedisPool.getResource()) {
            JedisLock jedisLock1 = new JedisLock(jedisPool, keyName, 2L, TimeUnit.SECONDS);
            boolean result1 = jedisLock1.tryLock();
            assertTrue(jedisLock1.isLocked());
            assertTrue(result1);
            assertEquals(getJedisLockUniqueToken(jedisLock1), jedis.get(jedisLock1.getName()));
            JedisLock jedisLock2 = new JedisLock(jedisPool, keyName);
            boolean result2 = jedisLock2.tryLockForAWhile(1L, TimeUnit.SECONDS);
            assertFalse(jedisLock2.isLocked());
            assertFalse(result2);
            jedisLock1.addMoreExpireTimeToCurrentLock(2L, TimeUnit.SECONDS);
            Thread.sleep(2500);
            assertTrue(jedisLock1.isLocked());
            Thread.sleep(2500);
            assertFalse(jedisLock1.isLocked());
        }
    }

    @Test(timeout = 35000)
    public void testMantainLockWithoutUpdatedTime() throws InterruptedException {
        Semaphore sem2 = new Semaphore(0);
        JedisLock jedisLock1 = new JedisLock(jedisPool, keyName, 2L, TimeUnit.SECONDS);
        boolean lockResult1 = jedisLock1.tryLock();
        AtomicBoolean lockResult2 = new AtomicBoolean(false);
        Thread backgroundLock = new Thread(() -> {
            try {
                JedisLock jedisLock2 = new JedisLock(jedisPool, keyName, 2L, TimeUnit.SECONDS);
                boolean result = jedisLock2.tryLockForAWhile(3L, TimeUnit.SECONDS);
                lockResult2.set(result);
                sem2.release();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        backgroundLock.start();
        sem2.acquire();
        assertTrue(lockResult1);
        assertTrue(lockResult2.get());
    }


    @Test(timeout = 35000)
    public void testMantainLockWithUpdatedTime() throws InterruptedException {
        Semaphore sem1 = new Semaphore(0);
        Semaphore sem2 = new Semaphore(0);
        JedisLock jedisLock1 = new JedisLock(jedisPool, keyName, 2L, TimeUnit.SECONDS);
        boolean lockResult1 = jedisLock1.tryLock();
        AtomicBoolean lockResult2 = new AtomicBoolean(false);
        Thread backgroundLock  = new Thread(() -> {
            try {
                JedisLock jedisLock2 = new JedisLock(jedisPool, keyName, 2L, TimeUnit.SECONDS);
                sem1.release();
                boolean result = jedisLock2.tryLockForAWhile(3L, TimeUnit.SECONDS);
                lockResult2.set(result);
                sem2.release();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        backgroundLock.start();
        sem1.acquire();
        jedisLock1.addMoreExpireTimeToCurrentLock(1800L);
        sem2.acquire();
        assertTrue(lockResult1);
        assertFalse(lockResult2.get());
    }

    // To allow deeper testing
    @SuppressWarnings("All")
    public static String getJedisLockUniqueToken(JedisLock jedisLock) {
        try {
            Method privateMethod = JedisLock.class.getDeclaredMethod("getUniqueToken", null);
            privateMethod.setAccessible(true);
            return (String) privateMethod.invoke(jedisLock, null);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Error in getJedisLockUniqueToken", e);
        }
    }

}
