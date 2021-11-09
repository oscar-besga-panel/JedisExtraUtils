package org.obapanel.jedis.semaphore.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.common.test.JedisTestFactory;
import org.obapanel.jedis.semaphore.JedisSemaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class FunctionalSemaphoreWaitingTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalSemaphoreWaitingTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private JedisPool jedisPool;
    private String semaphoreName;


    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPool = jtfTest.createJedisPool();
        semaphoreName = "semaphore:" + this.getClass().getName() + ":" + System.currentTimeMillis();
    }

    @After
    public void after() throws IOException {
        if (!jtfTest.functionalTestEnabled()) return;
        if (jedisPool != null) {
            jedisPool.close();
        }
    }


    @Test
    public void tesSemaphore() {
        JedisSemaphore jedisSemaphore = new JedisSemaphore(jedisPool, semaphoreName,0);
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
                JedisSemaphore jedisSemaphore1 = new JedisSemaphore(jedisPool, semaphoreName,0);
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
                JedisSemaphore jedisSemaphore2 = new JedisSemaphore(jedisPool, semaphoreName,0);
                Thread.sleep(1500);
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
        t1.join(5000);
        t2.join(5000);
        assertTrue(acquired.get());
        assertTrue(released.get());
    }


    @Test
    public void testNumOfPermits2() throws InterruptedException {
        AtomicBoolean acquired = new AtomicBoolean(false);
        AtomicBoolean acquiredEnd = new AtomicBoolean(false);
        AtomicBoolean released = new AtomicBoolean(false);
        Thread t1 = new Thread(() ->{
            try {
                JedisSemaphore jedisSemaphore1 = new JedisSemaphore(jedisPool, semaphoreName,0);
                LOGGER.debug("FunctionalMessageSemaphoreTest_THREAD1 waiting for 1 permit");
                System.out.println("FunctionalMessageSemaphoreTest_THREAD1 waiting 1 permit");
                boolean tried = jedisSemaphore1.tryAcquire(1,2500, TimeUnit.MILLISECONDS);
                LOGGER.debug("FunctionalMessageSemaphoreTest_THREAD1 waiting for 1 permit DONE");
                System.out.println("FunctionalMessageSemaphoreTest_THREAD1 waiting 1 permit DONE");
                acquired.set(tried);
                acquiredEnd.set(true);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        t1.setName("FunctionalMessageSemaphoreTest_THREAD1");
        t1.start();
        Thread t2 = new Thread(() ->{
            try {
                JedisSemaphore jedisSemaphore2 = new JedisSemaphore(jedisPool, semaphoreName,0);
                Thread.sleep(1500);
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
        t1.join(5000);
        t2.join(5000);
        assertTrue(acquired.get());
        assertTrue(acquiredEnd.get());
        assertTrue(released.get());
    }

    @Test
    public void testNumOfPermits3() throws InterruptedException {
        AtomicBoolean acquired = new AtomicBoolean(false);
        AtomicBoolean acquiredEnd = new AtomicBoolean(false);
        AtomicBoolean released = new AtomicBoolean(false);
        Thread t1 = new Thread(() ->{
            try {
                JedisSemaphore jedisSemaphore1 = new JedisSemaphore(jedisPool, semaphoreName,0);
                LOGGER.debug("FunctionalMessageSemaphoreTest_THREAD1 waiting for 1 permit");
                System.out.println("FunctionalMessageSemaphoreTest_THREAD1 waiting 1 permit");
                boolean tried = jedisSemaphore1.tryAcquire(1,500, TimeUnit.MILLISECONDS);
                LOGGER.debug("FunctionalMessageSemaphoreTest_THREAD1 waiting for 1 permit DONE");
                System.out.println("FunctionalMessageSemaphoreTest_THREAD1 waiting 1 permit DONE");
                acquired.set(tried);
                acquiredEnd.set(true);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        t1.setName("FunctionalMessageSemaphoreTest_THREAD1");
        t1.start();
        Thread t2 = new Thread(() ->{
            try {
                JedisSemaphore jedisSemaphore2 = new JedisSemaphore(jedisPool, semaphoreName,0);
                Thread.sleep(2500);
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
        t1.join(5000);
        t2.join(5000);
        assertFalse(acquired.get());
        assertTrue(acquiredEnd.get());
        assertTrue(released.get());
    }


}
