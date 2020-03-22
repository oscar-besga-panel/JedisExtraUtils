package org.obapanel.jedis.interruptinglocks;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.obapanel.jedis.interruptinglocks.MockOfJedis.*;


public class JedisLocksOnCriticalZoneTest {


    private static final Logger log = LoggerFactory.getLogger(JedisLocksOnCriticalZoneTest.class);


    private AtomicBoolean intoCriticalZone = new AtomicBoolean(false);
    private AtomicBoolean errorInCriticalZone = new AtomicBoolean(false);
    private AtomicBoolean otherErrors = new AtomicBoolean(false);
    private String lockName;
    private List<JedisLock> lockList = new ArrayList<>();
    private MockOfJedis mockOfJedis;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(integrationTestEnabled());
        if (!integrationTestEnabled()) return;
        lockName = "lock:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        mockOfJedis = new MockOfJedis();
    }

    @After
    public void after() {
        if (mockOfJedis != null) mockOfJedis.clearData();
    }

    @Test
    public void testIfInterruptedFor5SecondsLock() throws InterruptedException {
        for(int i = 0; i < INTEGRATION_TEST_CYCLES; i++) {
            intoCriticalZone.set(false);
            errorInCriticalZone.set(false);
            otherErrors.set(false);
            log.info("i {}", i);
            Thread t1 = new Thread(() -> accesLockOfCriticalZone(1));
            t1.setName("prueba_t1");
            Thread t2 = new Thread(() -> accesLockOfCriticalZone(7));
            t2.setName("prueba_t2");
            Thread t3 = new Thread(() -> accesLockOfCriticalZone(3));
            t3.setName("prueba_t3");
            List<Thread> threadList = Arrays.asList(t1,t2,t3);
            Collections.shuffle(threadList);
            threadList.forEach(Thread::start);
//            t1.start();
//            t2.start();
//            t3.start();
            //Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            t1.join();
            t2.join();
            t3.join();
            assertFalse(errorInCriticalZone.get());
            assertFalse(otherErrors.get());
            assertFalse(lockList.stream().anyMatch(il -> il != null && il.isLocked()));
        }
    }

    private void accesLockOfCriticalZone(int sleepTime) {
        try {
            Jedis jedis = mockOfJedis.getJedis();
            JedisLock jedisLock = new JedisLock(jedis, lockName);
            lockList.add(jedisLock);
            jedisLock.lock();
            checkLock(jedisLock);
            accessCriticalZone(sleepTime);
            jedisLock.unlock();
            jedis.quit();
        } catch (Exception e){
            otherErrors.set(true);
            log.error("Other error ", e);
        }
    }

    /*
    private void accessCriticalZone(int sleepTime){
        if (intoCriticalZone.get()) {
            errorInCriticalZone.set(true);
            throw new IllegalStateException("Other thread is here");
        }
        intoCriticalZone.set(true);
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(sleepTime));
        } catch (InterruptedException e) {
            //Noop
        }
        intoCriticalZone.set(false);
    }
    */

    private void accessCriticalZone(int sleepTime){
        log.info("accessCriticalZone > enter  > " + Thread.currentThread().getName());
        if (intoCriticalZone.get()) {
            errorInCriticalZone.set(true);
            throw new IllegalStateException("Other thread is here " + Thread.currentThread().getName());
        }
        try {
            log.info("accessCriticalZone > bef true  > " + Thread.currentThread().getName());
            intoCriticalZone.set(true);
            log.info("accessCriticalZone > aft true  > " + Thread.currentThread().getName());
            Thread.sleep(TimeUnit.SECONDS.toMillis(sleepTime));
        } catch (InterruptedException e) {
            //NOOP
        } finally {
            log.info("accessCriticalZone > bef false > " + Thread.currentThread().getName());
            intoCriticalZone.set(false);
            log.info("accessCriticalZone > aft false > " + Thread.currentThread().getName());
        }
        log.info("accessCriticalZone > exit   > " + Thread.currentThread().getName());
    }
}
