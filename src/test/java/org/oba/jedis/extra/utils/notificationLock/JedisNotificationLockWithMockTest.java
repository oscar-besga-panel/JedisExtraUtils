package org.oba.jedis.extra.utils.notificationLock;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Transaction;


import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Transaction.class })
public class JedisNotificationLockWithMockTest {

    private MockOfJedis mockOfJedis;
    private JedisPooled jedisPooled;

    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(MockOfJedis.unitTestEnabled());
        if (!MockOfJedis.unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
        jedisPooled = mockOfJedis.getJedisPooled();
    }

    @After
    public void tearDown() {
        if (mockOfJedis != null) {
            mockOfJedis.clearData();
        }
        if (jedisPooled != null) {
            jedisPooled.close();
        }
    }

    @Test
    public void testLock() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        String lockname = getUniqueName();
        NotificationLock jedisLock = new NotificationLock(jedisPooled,lockname);
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
        NotificationLock jedisLock1 = new NotificationLock(jedisPooled,lockname);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        assertEquals(jedisLock1.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        NotificationLock jedisLock2 = new NotificationLock(jedisPooled,lockname);
        boolean result2 = jedisLock2.tryLock();
        assertFalse(jedisLock2.isLocked());
        assertFalse(result2);
        assertNotEquals(jedisLock2.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock2.getName()));
        jedisLock1.unlock();
    }

    @Test(timeout = 15000)
    public void testTryLockForAWhile() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        String lockname = getUniqueName();
        NotificationLock jedisLock1 = new NotificationLock(jedisPooled,lockname);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        assertEquals(jedisLock1.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        NotificationLock jedisLock2 = new NotificationLock(jedisPooled,lockname);
        boolean result2 = jedisLock2.tryLockForAWhile(1, TimeUnit.SECONDS);
        assertFalse(jedisLock2.isLocked());
        assertFalse(result2);
        assertNotEquals(jedisLock2.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock2.getName()));
        jedisLock1.unlock();
    }

    @Test
    public void testLockInterruptibly() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        String lockname = getUniqueName();
        NotificationLock jedisLock1 = new NotificationLock(jedisPooled, lockname);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        assertEquals(jedisLock1.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        NotificationLock jedisLock2 = new NotificationLock(jedisPooled, lockname);
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
        NotificationLock jedisLock1 = new NotificationLock(jedisPooled, lockname);
        boolean result1 = jedisLock1.tryLock();
        assertTrue(jedisLock1.isLocked());
        assertTrue(result1);
        assertEquals(jedisLock1.getUniqueToken(), mockOfJedis.getCurrentData().get(jedisLock1.getName()));
        NotificationLock jedisLock2 = new NotificationLock(jedisPooled, lockname);
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
    public void testEqualsAndHashcode() {
        String lockname = getUniqueName();
        NotificationLock jedisLock1 = new NotificationLock(jedisPooled, lockname);
        NotificationLock jedisLock2 = new NotificationLock(jedisPooled, lockname);
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
