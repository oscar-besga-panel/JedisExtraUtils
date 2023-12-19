package org.oba.jedis.extra.utils.notificationLock.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.interruptinglocks.functional.JedisTestFactoryLocks;
import org.oba.jedis.extra.utils.notificationLock.NotificationLock;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;


public class FunctionalJedisNotificationLocksOnCriticalZoneUnderlockTest {


    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalJedisNotificationLocksOnCriticalZoneUnderlockTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private final AtomicBoolean intoCriticalZone = new AtomicBoolean(false);
    private final AtomicBoolean errorInCriticalZone = new AtomicBoolean(false);
    private final AtomicBoolean otherError = new AtomicBoolean(false);

    private JedisPool jedisPool;
    private String lockName;
    private final List<NotificationLock> lockList = new ArrayList<>();


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        lockName = "flock:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        jedisPool = jtfTest.createJedisPool();
    }

    @After
    public void after() {
        if (!jtfTest.functionalTestEnabled()) return;
        lockList.stream().
                filter(Objects::nonNull).
                forEach(il -> {
                    if (il.isLocked()) {
                        LOGGER.error("A lock named {} is locked !", il.getName());
                    }
                    il.unlock();
        });
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    @Test
    public void testIfInterruptedFor5SecondsLock() throws InterruptedException {
        for(int i = 0; i < jtfTest.getFunctionalTestCycles(); i++) {
            intoCriticalZone.set(false);
            errorInCriticalZone.set(false);
            otherError.set(false);
            LOGGER.info("_\n");
            LOGGER.info("i {}", i);
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
            assertFalse(otherError.get());
            assertFalse(lockList.stream().anyMatch(il -> il != null && il.isLocked()));
        }
    }

    private void accesLockOfCriticalZone(int sleepTime) {
        try {
            NotificationLock jedisLock = new NotificationLock(jedisPool, lockName);
            lockList.add(jedisLock);
            jedisLock.underLock(() -> {
                JedisTestFactoryLocks.checkLock(jedisLock);
                accessCriticalZone(sleepTime);
            });
        } catch (Exception e){
            LOGGER.error("Error ", e);
            otherError.set(true);
        }
    }

    private void accessCriticalZone(int sleepTime){
        if (intoCriticalZone.get()) {
            errorInCriticalZone.set(true);
            throw new IllegalStateException("Other thread is here, I am " + Thread.currentThread().getName());
        }
        intoCriticalZone.set(true);
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(sleepTime));
        } catch (InterruptedException e) {
            //NOPE
        }
        intoCriticalZone.set(false);
    }
}
