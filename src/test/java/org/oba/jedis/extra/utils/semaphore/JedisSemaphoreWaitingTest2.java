package org.oba.jedis.extra.utils.semaphore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class JedisSemaphoreWaitingTest2 {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisSemaphoreWaitingTest2.class);

    private MockOfJedis2 mockOfJedis;
    private JedisPooled jedisPooled;
    private String semaphoreName;


    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(MockOfJedis.unitTestEnabled());
        if (!MockOfJedis.unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis2();
        jedisPooled = mockOfJedis.getJedisPooled();
        semaphoreName = "semaphore:" + this.getClass().getName() + ":" + System.currentTimeMillis();
    }

    @After
    public void after() throws IOException {
        jedisPooled.del(semaphoreName);
        jedisPooled.close();
        mockOfJedis.clearData();
    }


    @Test
    public void tesSemaphore() {
        JedisSemaphore2 jedisSemaphore = new JedisSemaphore2(jedisPooled, semaphoreName,0);
        assertEquals(semaphoreName, jedisSemaphore.getName());
        jedisSemaphore.destroy();
        assertEquals(-1, jedisSemaphore.availablePermits());
    }

    @Test
    public void testNumOfPermits1() throws InterruptedException {
        AtomicBoolean acquired = new AtomicBoolean(false);
        AtomicBoolean released = new AtomicBoolean(false);
        Thread t1 = new Thread(() ->{
            try {
                JedisSemaphore2 jedisSemaphore1 = new JedisSemaphore2(jedisPooled, semaphoreName,0).
                    withWaitingMilis(25);
                LOGGER.debug("FunctionalMessageSemaphoreTest_THREAD1 waiting for 1 permit");
                System.out.println("FunctionalMessageSemaphoreTest_THREAD1 waiting 1 permit");
                jedisSemaphore1.acquire(1);
                LOGGER.debug("FunctionalMessageSemaphoreTest_THREAD1 waiting for 1 permit DONE");
                System.out.println("FunctionalMessageSemaphoreTest_THREAD1 waiting 1 permit DONE");
                acquired.set(true);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        t1.setName("FunctionalMessageSemaphoreTest_THREAD1");
        t1.start();
        Thread t2 = new Thread(() ->{
            try {
                JedisSemaphore2 jedisSemaphore2 = new JedisSemaphore2(jedisPooled, semaphoreName,0).
                        withWaitingMilis(25);
                Thread.sleep(150);
                LOGGER.debug("FunctionalMessageSemaphoreTest_THREAD2 releasing 1 permit");
                System.out.println("FunctionalMessageSemaphoreTest_THREAD2 releasing 1 permit");
                jedisSemaphore2.release();
                LOGGER.debug("FunctionalMessageSemaphoreTest_THREAD2 releasing 1 permit DONE");
                System.out.println("FunctionalMessageSemaphoreTest_THREAD2 releasing 1 permit DONE");
                released.set(true);
            } catch (Exception e) {
                e.printStackTrace();
            }

        });
        t2.setName("FunctionalMessageSemaphoreTest_THREAD2");
        t2.start();
        t1.join(500);
        t2.join(500);
        assertTrue(acquired.get());
        assertTrue(released.get());
    }


    @Test
    public void testNumOfPermits2() throws InterruptedException {
        AtomicBoolean tryAcquired = new AtomicBoolean(false);
        AtomicBoolean endAcquired = new AtomicBoolean(false);
        AtomicBoolean released = new AtomicBoolean(false);
        Thread t1 = new Thread(() ->{
            try {
                JedisSemaphore2 jedisSemaphore1 = new JedisSemaphore2(jedisPooled, semaphoreName,0).
                        withWaitingMilis(25);
                LOGGER.debug("FunctionalMessageSemaphoreTest_THREAD1 waiting for 1 permit");
                System.out.println("FunctionalMessageSemaphoreTest_THREAD1 waiting 1 permit");
                boolean tried = jedisSemaphore1.tryAcquire(1,250, TimeUnit.MILLISECONDS);
                LOGGER.debug("FunctionalMessageSemaphoreTest_THREAD1 waiting for 1 permit DONE");
                System.out.println("FunctionalMessageSemaphoreTest_THREAD1 waiting 1 permit DONE");
                tryAcquired.set(tried);
                endAcquired.set(true);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        t1.setName("FunctionalMessageSemaphoreTest_THREAD1");
        t1.start();
        Thread t2 = new Thread(() ->{
            try {
                JedisSemaphore2 jedisSemaphore2 = new JedisSemaphore2(jedisPooled, semaphoreName,0).
                        withWaitingMilis(25);
                Thread.sleep(150);
                LOGGER.debug("FunctionalMessageSemaphoreTest_THREAD2 releasing 1 permit");
                System.out.println("FunctionalMessageSemaphoreTest_THREAD2 releasing 1 permit");
                jedisSemaphore2.release();
                LOGGER.debug("FunctionalMessageSemaphoreTest_THREAD2 releasing 1 permit DONE");
                System.out.println("FunctionalMessageSemaphoreTest_THREAD2 releasing 1 permit DONE");
                released.set(true);
            } catch (Exception e) {
                e.printStackTrace();
            }

        });
        t2.setName("FunctionalMessageSemaphoreTest_THREAD2");
        t2.start();
        t1.join(500);
        t2.join(500);
        assertTrue(tryAcquired.get());
        assertTrue(endAcquired.get());
        assertTrue(released.get());
    }

    @Test
    public void testNumOfPermits3() throws InterruptedException {
        AtomicBoolean tryAcquired = new AtomicBoolean(false);
        AtomicBoolean endAcquired = new AtomicBoolean(false);
        AtomicBoolean released = new AtomicBoolean(false);
        Thread t1 = new Thread(() ->{
            try {
                JedisSemaphore2 jedisSemaphore1 = new JedisSemaphore2(jedisPooled, semaphoreName,0)
                    .withWaitingMilis(25);
                LOGGER.debug("FunctionalMessageSemaphoreTest_THREAD1 waiting for 1 permit");
                System.out.println("FunctionalMessageSemaphoreTest_THREAD1 waiting 1 permit");
                boolean tried = jedisSemaphore1.tryAcquire(1,50, TimeUnit.MILLISECONDS);
                LOGGER.debug("FunctionalMessageSemaphoreTest_THREAD1 waiting for 1 permit DONE");
                System.out.println("FunctionalMessageSemaphoreTest_THREAD1 waiting 1 permit DONE");
                tryAcquired.set(tried);
                endAcquired.set(true);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        t1.setName("FunctionalMessageSemaphoreTest_THREAD1");
        t1.start();
        Thread t2 = new Thread(() ->{
            try {
                JedisSemaphore2 jedisSemaphore2 = new JedisSemaphore2(jedisPooled, semaphoreName,0).
                    withWaitingMilis(25);
                Thread.sleep(250);
                LOGGER.debug("FunctionalMessageSemaphoreTest_THREAD2 releasing 1 permit");
                System.out.println("FunctionalMessageSemaphoreTest_THREAD2 releasing 1 permit");
                jedisSemaphore2.release();
                LOGGER.debug("FunctionalMessageSemaphoreTest_THREAD2 releasing 1 permit DONE");
                System.out.println("FunctionalMessageSemaphoreTest_THREAD2 releasing 1 permit DONE");
                released.set(true);
            } catch (Exception e) {
                e.printStackTrace();
            }

        });
        t2.setName("FunctionalMessageSemaphoreTest_THREAD2");
        t2.start();
        t1.join(500);
        t2.join(500);
        assertFalse(tryAcquired.get());
        assertTrue(endAcquired.get());
        assertTrue(released.get());
    }

}
