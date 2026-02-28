package org.oba.jedis.extra.utils.notificationLock.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.oba.jedis.extra.utils.notificationLock.NotificationLock;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.UnifiedJedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.oba.jedis.extra.utils.iterators.ScanUtil.deleteListOfKeysStartsWith;


public class FunctionalJedisNotificationLocksOnCriticalZoneWithWaitingTimeTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalJedisNotificationLocksOnCriticalZoneWithWaitingTimeTest.class);

    private static final String COMMON_REDIS_TEST_NAME = "lock:" + FunctionalJedisNotificationLocksOnCriticalZoneWithWaitingTimeTest.class.getName() + ":";

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private final AtomicBoolean intoCriticalZone = new AtomicBoolean(false);
    private final AtomicBoolean errorInCriticalZone = new AtomicBoolean(false);
    private final AtomicBoolean otherError = new AtomicBoolean(false);

    private String lockName;
    private UnifiedJedis redisClient;
    private final List<NotificationLock> lockList = new ArrayList<>();


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        lockName = COMMON_REDIS_TEST_NAME + System.currentTimeMillis();
        redisClient = jtfTest.createRedisClient();
    }

    @After
    public void after() {
        if (!jtfTest.functionalTestEnabled()) return;
        if (redisClient != null) {
            redisClient.del(lockName);
            deleteListOfKeysStartsWith(redisClient, COMMON_REDIS_TEST_NAME);
            redisClient.close();
        }
    }

    @Ignore
    @Test(timeout = 35000)
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
            assertFalse(lockList.stream().anyMatch(il ->  il != null && il.isLocked() ));
        }
    }

    private void accesLockOfCriticalZone(int sleepTime) {
        try {
            NotificationLock jedisLock = new NotificationLock(redisClient, lockName);
            lockList.add(jedisLock);
            try {
                boolean locked = jedisLock.tryLockForAWhile(3, TimeUnit.SECONDS);
                if (locked) {
                    accessCriticalZone(sleepTime);
                    jedisLock.unlock();
                }
            } catch (InterruptedException e) {
                // NOOP
            }
        } catch (Exception e) {
            LOGGER.error("Other error", e);
            otherError.set(true);
        }
    }

    private void accessCriticalZone(int sleepTime){
        if (intoCriticalZone.get()) {
            errorInCriticalZone.set(true);
            throw new IllegalStateException("Other thread is here");
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
