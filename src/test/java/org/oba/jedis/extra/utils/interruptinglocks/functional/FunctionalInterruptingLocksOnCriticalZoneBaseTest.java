package org.oba.jedis.extra.utils.interruptinglocks.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.interruptinglocks.InterruptingJedisJedisLockBase;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.oba.jedis.extra.utils.test.WithJedisPoolDelete;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;


public class FunctionalInterruptingLocksOnCriticalZoneBaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalInterruptingLocksOnCriticalZoneBaseTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private final AtomicBoolean intoCriticalZone = new AtomicBoolean(false);
    private final AtomicBoolean errorInCriticalZone = new AtomicBoolean(false);
    private final AtomicBoolean otherError = new AtomicBoolean(false);
    private String lockName;
    private final List<InterruptingJedisJedisLockBase> interruptingLockBaseList = new ArrayList<>();
    private JedisPool jedisPool;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPool = jtfTest.createJedisPool();
        lockName = "flock:" + this.getClass().getName() + ":lockT" + System.currentTimeMillis();
    }

    @After
    public void after() {
        if (!jtfTest.functionalTestEnabled()) return;
        interruptingLockBaseList.stream().
                filter(il ->  il != null ).
                forEach(il -> {
                    if (il.isLocked()) {
                        LOGGER.error("A lock named {} is locked !", il.getName());
                    }
                    il.unlock();
        });
        if (jedisPool!= null) {
            WithJedisPoolDelete.doDelete(jedisPool, lockName);
            jedisPool.close();
        }

    }

    @Test
    public void testIfInterruptedFor5SecondsLock() throws InterruptedException {
        for (int i = 0; i < jtfTest.getFunctionalTestCycles(); i ++) {
            errorInCriticalZone.set(false);
            otherError.set(false);
            intoCriticalZone.set(false);
            LOGGER.info("_\n");
            LOGGER.info("FUNCTIONAL_TEST_CYCLES " + i);
            Thread t1 = new Thread(() -> accesLockOfCriticalZone(1));
            t1.setName("T1_1s_i"+i);
            Thread t2 = new Thread(() -> accesLockOfCriticalZone(7));
            t2.setName("T2_7s_i"+i);
            Thread t3 = new Thread(() -> accesLockOfCriticalZone(3));
            t3.setName("T3_3s_i"+i);
            List<Thread> threadList = Arrays.asList(t1,t2,t3);
            Collections.shuffle(threadList);
            threadList.forEach(Thread::start);
            t1.join();
            t2.join();
            t3.join();
            assertFalse(errorInCriticalZone.get());
            assertFalse(otherError.get());
            assertFalse(interruptingLockBaseList.stream().anyMatch(il -> il != null && il.isLocked()));
        }
    }

    private void accesLockOfCriticalZone(int sleepTime){
        try {
            InterruptingJedisJedisLockBase interruptingJedisJedisLockBase = new InterruptingJedisJedisLockBase(jedisPool, lockName, 5, TimeUnit.SECONDS);
            interruptingJedisJedisLockBase.lock();
            interruptingLockBaseList.add(interruptingJedisJedisLockBase);
            boolean c = JedisTestFactoryLocks.checkLock(interruptingJedisJedisLockBase);
            if (c) {
                accessCriticalZone(sleepTime);
            }
            interruptingJedisJedisLockBase.unlock();
        } catch (Exception e) {
            e.printStackTrace();
            otherError.set(true);
        }
    }

    private void accessCriticalZone(int sleepTime){
        if (intoCriticalZone.get()) {
            errorInCriticalZone.set(true);
            throw new IllegalStateException("Other thread is here " + Thread.currentThread().getName() + " " + Thread.currentThread().getId());
        }
        intoCriticalZone.set(true);
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(sleepTime));
        } catch (InterruptedException e) {
            //NOOP
        }
        intoCriticalZone.set(false);
    }
}
