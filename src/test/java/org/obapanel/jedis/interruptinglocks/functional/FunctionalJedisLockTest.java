package org.obapanel.jedis.interruptinglocks.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.common.test.JedisTestFactory;
import org.obapanel.jedis.interruptinglocks.JedisLock;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.interruptinglocks.MockOfJedis.getJedisLockValue;

public class FunctionalJedisLockTest {

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private JedisPool jedisPool;
    private String keyName;

    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPool = jtfTest.createJedisPool();
        keyName = "lock:" + this.getClass().getName() + ":" + System.currentTimeMillis();

    }

    @After
    public void tearDown() {
        if (!jtfTest.functionalTestEnabled()) return;
        if (jedisPool != null) {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.del(keyName);
            }
            jedisPool.close();
        }
    }

    @Test
    public void testLock() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        try (Jedis jedis = jedisPool.getResource()) {
            if (!jtfTest.functionalTestEnabled()) return;
            JedisLock jedisLock = new JedisLock(jedisPool, keyName);
            jedisLock.lock();
            assertTrue(jedisLock.isLocked());
            assertEquals(getJedisLockValue(jedisLock), jedis.get(jedisLock.getName()));
            jedisLock.unlock();
            assertFalse(jedisLock.isLocked());
            assertNull(jedis.get(jedisLock.getName()));
        }
    }

    @Test
    public void testTryLock() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        try (Jedis jedis = jedisPool.getResource()) {
            if (!jtfTest.functionalTestEnabled()) return;
            JedisLock jedisLock1 = new JedisLock(jedisPool, keyName);
            boolean result1 = jedisLock1.tryLock();
            assertTrue(jedisLock1.isLocked());
            assertTrue(result1);
            assertEquals(getJedisLockValue(jedisLock1), jedis.get(jedisLock1.getName()));
            JedisLock jedisLock2 = new JedisLock(jedisPool, keyName);
            boolean result2 = jedisLock2.tryLock();
            assertFalse(jedisLock2.isLocked());
            assertFalse(result2);
            assertNotEquals(getJedisLockValue(jedisLock2), jedis.get(jedisLock2.getName()));
            jedisLock1.unlock();
        }
    }

    @Test
    public void testTryLockForAWhile() throws InterruptedException {
        if (!jtfTest.functionalTestEnabled()) return;
        JedisLock jedisLock1 = new JedisLock(jedisPool, keyName);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        JedisLock jedisLock2 = new JedisLock(jedisPool, keyName);
        boolean result2 = jedisLock2.tryLockForAWhile(1, TimeUnit.SECONDS);
        assertFalse(jedisLock2.isLocked());
        assertFalse(result2);
        jedisLock1.unlock();
    }

    @Test
    public void testLockInterruptibly() throws InterruptedException {
        if (!jtfTest.functionalTestEnabled()) return;
        JedisLock jedisLock1 = new JedisLock(jedisPool, keyName);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        JedisLock jedisLock2 = new JedisLock(jedisPool, keyName);
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
        JedisLock jedisLock1 = new JedisLock(jedisPool, keyName);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        JedisLock jedisLock2 = new JedisLock(jedisPool, keyName);
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

    @Test
    public void testOneLockWithLeaseTime() throws InterruptedException {
        if (!jtfTest.functionalTestEnabled()) return;
        JedisLock jedisLock1 = new JedisLock(jedisPool,  keyName, 5L, TimeUnit.SECONDS);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(result1);
        assertTrue(jedisLock1.isLocked());
        Thread.sleep(5500);
        assertFalse(jedisLock1.isLocked());
    }

    @Test
    public void testLocksWithLeaseTime() throws InterruptedException {
        if (!jtfTest.functionalTestEnabled()) return;
        JedisLock jedisLock1 = new JedisLock(jedisPool, keyName,5L, TimeUnit.SECONDS);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        JedisLock jedisLock2 = new JedisLock(jedisPool, keyName);
        boolean result2 = jedisLock2.tryLockForAWhile(1, TimeUnit.SECONDS);
        assertFalse(jedisLock2.isLocked());
        assertFalse(result2);
        Thread.sleep(5000);
        JedisLock jedisLock3 = new JedisLock(jedisPool, keyName);
        boolean result3 = jedisLock3.tryLockForAWhile(1, TimeUnit.SECONDS);
        assertTrue(jedisLock3.isLocked());
        assertTrue(result3);
        assertFalse(jedisLock1.isLocked());
        jedisLock1.unlock();
        jedisLock3.unlock();
    }


}
