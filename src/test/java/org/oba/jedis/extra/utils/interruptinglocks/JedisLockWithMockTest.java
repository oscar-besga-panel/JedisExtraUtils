package org.oba.jedis.extra.utils.interruptinglocks;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;


import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Transaction.class })
public class JedisLockWithMockTest {

    private MockOfJedis mockOfJedis;
    private Jedis jedis;
    private JedisPool jedisPool;

    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(MockOfJedis.unitTestEnabled());
        if (!MockOfJedis.unitTestEnabled()) return;
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
        JedisLock jedisLock = new JedisLock(jedisPool,lockname);
        jedisLock.lock();
        assertTrue(jedisLock.isLocked());
        assertEquals(jedisLock.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock.getName()));
        jedisLock.unlock();
        assertFalse(jedisLock.isLocked());
        assertNull(mockOfJedis.getCurrentData().get(jedisLock.getName()));
    }

    @Test
    public void testTryLock() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        String lockname = getUniqueName();
        JedisLock jedisLock1 = new JedisLock(jedisPool,lockname);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        assertEquals(jedisLock1.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        JedisLock jedisLock2 = new JedisLock(jedisPool,lockname);
        boolean result2 = jedisLock2.tryLock();
        assertFalse(jedisLock2.isLocked());
        assertFalse(result2);
        assertNotEquals(jedisLock2.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock2.getName()));
        jedisLock1.unlock();
    }

    @Test
    public void testTryLockForAWhile() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        String lockname = getUniqueName();
        JedisLock jedisLock1 = new JedisLock(jedisPool,lockname);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        assertEquals(jedisLock1.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        JedisLock jedisLock2 = new JedisLock(jedisPool,lockname);
        boolean result2 = jedisLock2.tryLockForAWhile(1, TimeUnit.SECONDS);
        assertFalse(jedisLock2.isLocked());
        assertFalse(result2);
        assertNotEquals(jedisLock2.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock2.getName()));
        jedisLock1.unlock();
    }

    @Test
    public void testLockInterruptibly() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        String lockname = getUniqueName();
        JedisLock jedisLock1 = new JedisLock(jedisPool, lockname);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        assertEquals(jedisLock1.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        JedisLock jedisLock2 = new JedisLock(jedisPool, lockname);
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
        assertNotEquals(jedisLock2.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock2.getName()));
        assertTrue(triedLock.get());
        assertTrue(interrupted.get());
        jedisLock1.unlock();
    }


    @Test
    public void testLockNotInterruptibly() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        String lockname = getUniqueName();
        JedisLock jedisLock1 = new JedisLock(jedisPool, lockname);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        assertEquals(jedisLock1.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        JedisLock jedisLock2 = new JedisLock(jedisPool, lockname);
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
        assertNotEquals(jedisLock2.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock2.getName()));
        assertTrue(triedLock.get());
        assertFalse(interrupted.get());
        jedisLock1.unlock();
    }

    @Test
    public void testOneLockWithLeaseTime() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        String lockname = getUniqueName();
        JedisLock jedisLock1 = new JedisLock(jedisPool, lockname, 5L, TimeUnit.SECONDS);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(result1);
        assertTrue(jedisLock1.isLocked());
        assertEquals(jedisLock1.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        Thread.sleep(5500);
        assertNotEquals(jedisLock1.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        assertNull(mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        assertFalse(jedisLock1.isLocked());
    }

    @Test
    public void testLocksWithLeaseTime() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        String lockname = getUniqueName();
        JedisLock jedisLock1 = new JedisLock(jedisPool, lockname,5L, TimeUnit.SECONDS);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        assertEquals(jedisLock1.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        JedisLock jedisLock2 = new JedisLock(jedisPool, lockname);
        boolean result2 = jedisLock2.tryLockForAWhile(1, TimeUnit.SECONDS);
        assertFalse(jedisLock2.isLocked());
        assertFalse(result2);
        assertNotEquals(jedisLock2.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock2.getName()));
        Thread.sleep(5000);
        JedisLock jedisLock3 = new JedisLock(jedisPool, lockname);
        boolean result3 = jedisLock3.tryLockForAWhile(1, TimeUnit.SECONDS);
        assertTrue(jedisLock3.isLocked());
        assertTrue(result3);
        assertEquals(jedisLock3.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock3.getName()));
        assertFalse(jedisLock1.isLocked());
        jedisLock1.unlock();
        jedisLock3.unlock();
    }

    @Test
    public void testAsConcurrentLock() throws InterruptedException {
        String lockname = getUniqueName();
        JedisLock jedisLock1 = new JedisLock(jedisPool, lockname);
        java.util.concurrent.locks.Lock lock1 = jedisLock1.asConcurrentLock();
        JedisLock jedisLock2 = new JedisLock(jedisPool, lockname);
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
        JedisLock jedisLock1 = new JedisLock(jedisPool, lockname);
        JedisLock jedisLock2 = new JedisLock(jedisPool, lockname);
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
