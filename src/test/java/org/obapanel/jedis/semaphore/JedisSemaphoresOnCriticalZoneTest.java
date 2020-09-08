package org.obapanel.jedis.semaphore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.obapanel.jedis.semaphore.MockOfJedis.UNIT_TEST_CYCLES;
import static org.obapanel.jedis.semaphore.MockOfJedis.unitTestEnabled;


public class JedisSemaphoresOnCriticalZoneTest {


    private static final Logger LOG = LoggerFactory.getLogger(JedisSemaphoresOnCriticalZoneTest.class);


    private AtomicBoolean intoCriticalZone = new AtomicBoolean(false);
    private AtomicBoolean errorInCriticalZone = new AtomicBoolean(false);
    private AtomicBoolean otherErrors = new AtomicBoolean(false);
    private String semaphoreName;
    private MockOfJedis mockOfJedis;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(unitTestEnabled());
        if (!unitTestEnabled()) return;
        semaphoreName = "semaphore:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        mockOfJedis = new MockOfJedis();
    }

    @After
    public void after() {
        if (mockOfJedis != null) mockOfJedis.clearData();
    }

    @Test
    public void testIfInterruptedFor5SecondsLock() throws InterruptedException {
        for(int i = 0; i < UNIT_TEST_CYCLES; i++) {
            intoCriticalZone.set(false);
            errorInCriticalZone.set(false);
            otherErrors.set(false);
            LOG.info("i {}", i);
            Thread t1 = new Thread(() -> accesLockOfCriticalZone(1));
            t1.setName("prueba_t1");
            Thread t2 = new Thread(() -> accesLockOfCriticalZone(7));
            t2.setName("prueba_t2");
            Thread t3 = new Thread(() -> accesLockOfCriticalZone(3));
            t3.setName("prueba_t3");
            List<Thread> threadList = Arrays.asList(t1,t2,t3);
            Collections.shuffle(threadList);
            threadList.forEach(Thread::start);
            t1.join();
            t2.join();
            t3.join();
            assertFalse(errorInCriticalZone.get());
            assertFalse(otherErrors.get());
        }
    }

    private void accesLockOfCriticalZone(int sleepTime) {
        try {
            Jedis jedis = mockOfJedis.getJedis();
            JedisSemaphore jedisSemaphore = new JedisSemaphore(jedis, semaphoreName, 1);
            jedisSemaphore.acquire();
            accessCriticalZone(sleepTime);
            jedisSemaphore.release();
            jedis.quit();
        } catch (Exception e){
            otherErrors.set(true);
            LOG.error("Other error ", e);
        }
    }

    private void accessCriticalZone(int sleepTime){
        LOG.info("accessCriticalZone > enter  > " + Thread.currentThread().getName());
        if (intoCriticalZone.get()) {
            errorInCriticalZone.set(true);
            throw new IllegalStateException("Other thread is here " + Thread.currentThread().getName());
        }
        try {
            LOG.info("accessCriticalZone > bef true  > " + Thread.currentThread().getName());
            intoCriticalZone.set(true);
            LOG.info("accessCriticalZone > aft true  > " + Thread.currentThread().getName());
            Thread.sleep(TimeUnit.SECONDS.toMillis(sleepTime));
        } catch (InterruptedException e) {
            //NOOP
        } finally {
            LOG.info("accessCriticalZone > bef false > " + Thread.currentThread().getName());
            intoCriticalZone.set(false);
            LOG.info("accessCriticalZone > aft false > " + Thread.currentThread().getName());
        }
        LOG.info("accessCriticalZone > exit   > " + Thread.currentThread().getName());
    }
}
