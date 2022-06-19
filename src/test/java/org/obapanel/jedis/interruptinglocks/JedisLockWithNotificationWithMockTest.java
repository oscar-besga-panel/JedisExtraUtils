package org.obapanel.jedis.interruptinglocks;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.interruptinglocks.MockOfJedis.getJedisLockValue;
import static org.obapanel.jedis.interruptinglocks.MockOfJedis.unitTestEnabled;

public class JedisLockWithNotificationWithMockTest {

    private MockOfJedis mockOfJedis;
    private Jedis jedis;
    private JedisPool jedisPool;

    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(unitTestEnabled());
        if (!unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
        jedis = mockOfJedis.getJedis();
        jedisPool = mockOfJedis.getJedisPool();
    }

    @After
    public void tearDown() {
        if (mockOfJedis != null) {
            mockOfJedis.clearData();
        }
        if (jedis != null) {
            jedis.close();
        }

        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    @Test
    public void testLock() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        String lockname = getUniqueName();
        JedisLockWithNotification jedisLock = new JedisLockWithNotification(jedisPool,lockname);
        jedisLock.lock();
        assertTrue(jedisLock.isLocked());
        assertEquals(getJedisLockValue(jedisLock), mockOfJedis.getCurrentData().get(jedisLock.getName()));
        jedisLock.unlock();
        assertFalse(jedisLock.isLocked());
        assertNull(mockOfJedis.getCurrentData().get(jedisLock.getName()));
    }

    @Test
    public void testTryLock() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        String lockname = getUniqueName();
        JedisLockWithNotification jedisLock1 = new JedisLockWithNotification(jedisPool,lockname);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        assertEquals(getJedisLockValue(jedisLock1), mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        JedisLockWithNotification jedisLock2 = new JedisLockWithNotification(jedisPool,lockname);
        boolean result2 = jedisLock2.tryLock();
        assertFalse(jedisLock2.isLocked());
        assertFalse(result2);
        assertNotEquals(getJedisLockValue(jedisLock2), mockOfJedis.getCurrentData().get(jedisLock2.getName()));
        jedisLock1.unlock();
    }

    @Test
    public void testTryLockForAWhile() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        String lockname = getUniqueName();
        JedisLockWithNotification jedisLock1 = new JedisLockWithNotification(jedisPool,lockname);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        assertEquals(getJedisLockValue(jedisLock1), mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        JedisLockWithNotification jedisLock2 = new JedisLockWithNotification(jedisPool,lockname);
        boolean result2 = jedisLock2.tryLockForAWhile(1, TimeUnit.SECONDS);
        assertFalse(jedisLock2.isLocked());
        assertFalse(result2);
        assertNotEquals(getJedisLockValue(jedisLock2), mockOfJedis.getCurrentData().get(jedisLock2.getName()));
        jedisLock1.unlock();
    }

    @Test
    public void testLockInterruptibly() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        String lockname = getUniqueName();
        JedisLockWithNotification jedisLock1 = new JedisLockWithNotification(jedisPool, lockname);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        assertEquals(getJedisLockValue(jedisLock1), mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        JedisLockWithNotification jedisLock2 = new JedisLockWithNotification(jedisPool, lockname);
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
        assertNotEquals(getJedisLockValue(jedisLock2), mockOfJedis.getCurrentData().get(jedisLock2.getName()));
        assertTrue(triedLock.get());
        assertTrue(interrupted.get());
        jedisLock1.unlock();
    }


    /**
     * TODO
     * This test with mock doesn't work
     * But the functional test works fine (FunctionalJedisLockWithNotificationTest.testLockNotInterruptibly)
     * So it is an mock implementation issue
     */
    @Ignore
    @Test
    public void testLockNotInterruptibly() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        String lockname = getUniqueName();
        JedisLockWithNotification jedisLock1 = new JedisLockWithNotification(jedisPool, lockname);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        assertEquals(getJedisLockValue(jedisLock1), mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        JedisLockWithNotification jedisLock2 = new JedisLockWithNotification(jedisPool, lockname);
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
        Thread.sleep(100);

        //assertFalse(jedisLock2.isLocked());
        assertNotEquals(getJedisLockValue(jedisLock2), mockOfJedis.getCurrentData().get(jedisLock2.getName()));
        assertTrue(triedLock.get());
        assertFalse(interrupted.get());
        jedisLock1.unlock();
    }

    @Test
    public void testOneLockWithLeaseTime() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        String lockname = getUniqueName();
        JedisLockWithNotification jedisLock1 = new JedisLockWithNotification(jedisPool, lockname, 5L, TimeUnit.SECONDS);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(result1);
        assertTrue(jedisLock1.isLocked());
        assertEquals(getJedisLockValue(jedisLock1), mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        Thread.sleep(5500);
        assertNotEquals(getJedisLockValue(jedisLock1), mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        assertNull(mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        assertFalse(jedisLock1.isLocked());
    }

    /**
     * TODO
     * This test with mock doesn't work
     * But the functional test works fine (FunctionalJedisLockWithNotificationTest.testLocksWithLeaseTime)
     * So it is an mock implementation issue
     */
    @Ignore
    @Test
    public void testLocksWithLeaseTime() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        String lockname = getUniqueName();
        JedisLockWithNotification jedisLock1 = new JedisLockWithNotification(jedisPool, lockname,5L, TimeUnit.SECONDS);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        assertEquals(getJedisLockValue(jedisLock1), mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        JedisLockWithNotification jedisLock2 = new JedisLockWithNotification(jedisPool, lockname);
        boolean result2 = jedisLock2.tryLockForAWhile(1, TimeUnit.SECONDS);
        assertFalse(jedisLock2.isLocked());
        assertFalse(result2);
        assertNotEquals(getJedisLockValue(jedisLock2), mockOfJedis.getCurrentData().get(jedisLock2.getName()));
        Thread.sleep(5000);
        JedisLockWithNotification jedisLock3 = new JedisLockWithNotification(jedisPool, lockname);
        boolean result3 = jedisLock3.tryLockForAWhile(1, TimeUnit.SECONDS);
        assertTrue(jedisLock3.isLocked());
        assertTrue(result3);
        assertEquals(getJedisLockValue(jedisLock3), mockOfJedis.getCurrentData().get(jedisLock3.getName()));
        assertFalse(jedisLock1.isLocked());
        jedisLock1.unlock();
        jedisLock3.unlock();
    }

    @Test
    public void testAsConcurrentLock() throws InterruptedException {
        String lockname = getUniqueName();
        JedisLockWithNotification jedisLock1 = new JedisLockWithNotification(jedisPool, lockname);
        java.util.concurrent.locks.Lock lock1 = jedisLock1.asConcurrentLock();
        JedisLockWithNotification jedisLock2 = new JedisLockWithNotification(jedisPool, lockname);
        java.util.concurrent.locks.Lock lock2 = jedisLock2.asConcurrentLock();
        boolean result1 = lock1.tryLock();
        boolean result2 = lock2.tryLock();
        Thread.sleep(5000);
        lock1.unlock();
        assertTrue(result1);
        assertFalse(result2);
    }

    @Test
    public void testEqualsAndHashcode() {
        String lockname = getUniqueName();
        JedisLockWithNotification jedisLock1 = new JedisLockWithNotification(jedisPool, lockname);
        JedisLockWithNotification jedisLock2 = new JedisLockWithNotification(jedisPool, lockname);
        assertNotEquals(jedisLock1, jedisLock2);
        assertNotEquals(jedisLock1.hashCode(), jedisLock2.hashCode());
        assertEquals(jedisLock1.getName(), jedisLock2.getName());
    }


    private static long lastCurrentTimeMilis = -1L;

    private synchronized static String getUniqueName(){
        long currentTimeMillis = System.currentTimeMillis();
        while(currentTimeMillis == lastCurrentTimeMilis){
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                //NOOP
            }
            currentTimeMillis = System.currentTimeMillis();
        }
        lastCurrentTimeMilis = currentTimeMillis;
        return "lock:K" + System.currentTimeMillis() + "_" + ThreadLocalRandom.current().nextInt(1_000_000);
    }

}
