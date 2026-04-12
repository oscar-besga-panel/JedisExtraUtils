package org.oba.jedis.extra.utils.notificationLock.functional;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.notificationLock.NotificationLock;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import redis.clients.jedis.UnifiedJedis;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.oba.jedis.extra.utils.iterators.ScanUtil.deleteListOfKeysStartsWith;

public class FunctionalJedisNotificationLockTest {

    private static final String COMMON_REDIS_TEST_NAME = "lock:" + FunctionalJedisNotificationLockTest.class.getName() + ":";

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private UnifiedJedis unifiedJedis;
    private String keyName;

    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        unifiedJedis = jtfTest.createRedisClient();
        keyName = "lock:" + this.getClass().getName() + ":" + System.currentTimeMillis();

    }

    @After
    public void tearDown() {
        if (!jtfTest.functionalTestEnabled()) return;
        if (unifiedJedis != null) {
            unifiedJedis.del(keyName);
            deleteListOfKeysStartsWith(unifiedJedis, COMMON_REDIS_TEST_NAME);
            unifiedJedis.close();
        }
    }

    @Test
    public void testLock() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        if (!jtfTest.functionalTestEnabled()) return;
        NotificationLock jedisLock = new NotificationLock(unifiedJedis, keyName);
        jedisLock.lock();
        assertTrue(jedisLock.isLocked());
        Assert.assertEquals(getJedisLockUniqueToken(jedisLock), unifiedJedis.get(jedisLock.getName()));
        jedisLock.unlock();
        assertFalse(jedisLock.isLocked());
        assertNull(unifiedJedis.get(jedisLock.getName()));
    }

    @Test
    public void testTryLock() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        if (!jtfTest.functionalTestEnabled()) return;
        NotificationLock jedisLock1 = new NotificationLock(unifiedJedis, keyName);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        Assert.assertEquals(getJedisLockUniqueToken(jedisLock1), unifiedJedis.get(jedisLock1.getName()));
        NotificationLock jedisLock2 = new NotificationLock(unifiedJedis, keyName);
        boolean result2 = jedisLock2.tryLock();
        assertFalse(jedisLock2.isLocked());
        assertFalse(result2);
        Assert.assertNotEquals(getJedisLockUniqueToken(jedisLock2), unifiedJedis.get(jedisLock2.getName()));
        jedisLock1.unlock();
    }

    @Test
    public void testTryLockForAWhile() throws InterruptedException {
        if (!jtfTest.functionalTestEnabled()) return;
        NotificationLock jedisLock1 = new NotificationLock(unifiedJedis, keyName);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        NotificationLock jedisLock2 = new NotificationLock(unifiedJedis, keyName);
        boolean result2 = jedisLock2.tryLockForAWhile(1, TimeUnit.SECONDS);
        assertFalse(jedisLock2.isLocked());
        assertFalse(result2);
        jedisLock1.unlock();
    }

    @Test
    public void testLockInterruptibly() throws InterruptedException {
        if (!jtfTest.functionalTestEnabled()) return;
        NotificationLock jedisLock1 = new NotificationLock(unifiedJedis, keyName);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        NotificationLock jedisLock2 = new NotificationLock(unifiedJedis, keyName);
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


    @Test
    public void testLockNotInterruptibly() throws InterruptedException {
        if (!jtfTest.functionalTestEnabled()) return;
        NotificationLock jedisLock1 = new NotificationLock(unifiedJedis, keyName);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        NotificationLock jedisLock2 = new NotificationLock(unifiedJedis, keyName);
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

    // To allow deeper testing
    @SuppressWarnings("All")
    public static String getJedisLockUniqueToken(NotificationLock jedisLock) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method privateMethod = NotificationLock.class.getDeclaredMethod("getUniqueToken", null);
        privateMethod.setAccessible(true);
        return (String) privateMethod.invoke(jedisLock, null);
    }

}
