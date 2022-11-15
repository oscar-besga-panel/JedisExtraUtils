package org.obapanel.jedis.interruptinglocks;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.obapanel.jedis.interruptinglocks.MockOfJedis.UNIT_TEST_CYCLES;
import static org.obapanel.jedis.interruptinglocks.MockOfJedis.checkLock;
import static org.obapanel.jedis.interruptinglocks.MockOfJedis.unitTestEnabled;


@RunWith(PowerMockRunner.class)
@PrepareForTest({Transaction.class, TransactionBase.class })
public class InterruptingLocksOnCriticalZoneBaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(InterruptingLocksOnCriticalZoneBaseTest.class);


    private final AtomicBoolean intoCriticalZone = new AtomicBoolean(false);
    private final AtomicBoolean errorInCriticalZone = new AtomicBoolean(false);
    private final AtomicBoolean otherError = new AtomicBoolean(false);
    private String lockName;
    private final List<InterruptingJedisJedisLockBase> interruptingLockBaseList = new ArrayList<>();

    private MockOfJedis mockOfJedis;

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(unitTestEnabled());
        if (!unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
        lockName = "lock:" + this.getClass().getName() + ":lockT" + System.currentTimeMillis();
    }

    @After
    public void after() {
        if (mockOfJedis!= null) {
            mockOfJedis.getJedisPool().close();
            mockOfJedis.clearData();
        }
        interruptingLockBaseList.stream().
                filter(Objects::nonNull).
                forEach(il -> {
                    if (il.isLocked()) {
                        LOGGER.error("A lock named {} is locked !", il.getName());
                    }
                    il.unlock();
        });
    }

    @Test
    public void testIfInterruptedFor5SecondsLock() throws InterruptedException {
        for (int i = 0; i < UNIT_TEST_CYCLES; i ++) {
            errorInCriticalZone.set(false);
            otherError.set(false);
            intoCriticalZone.set(false);
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
        JedisPool jedisPool = mockOfJedis.getJedisPool();
        InterruptingJedisJedisLockBase interruptingJedisJedisLockBase = new InterruptingJedisJedisLockBase(jedisPool, lockName, 5, TimeUnit.SECONDS);
        interruptingLockBaseList.add(interruptingJedisJedisLockBase);
        interruptingJedisJedisLockBase.lock();
        boolean c = checkLock(interruptingJedisJedisLockBase);
        if (c) {
            accessCriticalZone(sleepTime);
        }
        interruptingJedisJedisLockBase.unlock();
        interruptingLockBaseList.remove(interruptingJedisJedisLockBase);
    }

    private void accessCriticalZone(int sleepTime){
        LOGGER.info("accessCriticalZone > enter  > " + Thread.currentThread().getName());
        if (intoCriticalZone.get()) {
            errorInCriticalZone.set(true);
            throw new IllegalStateException("Other thread is here " + Thread.currentThread().getName());
        }
        try {
            LOGGER.info("accessCriticalZone > bef true  > " + Thread.currentThread().getName());
            intoCriticalZone.set(true);
            LOGGER.info("accessCriticalZone > aft true  > " + Thread.currentThread().getName());
            Thread.sleep(TimeUnit.SECONDS.toMillis(sleepTime));
        } catch (InterruptedException e) {
            //NOOP
        } finally {
            LOGGER.info("accessCriticalZone > bef false > " + Thread.currentThread().getName());
            intoCriticalZone.set(false);
            LOGGER.info("accessCriticalZone > aft false > " + Thread.currentThread().getName());
        }
        LOGGER.info("accessCriticalZone > exit   > " + Thread.currentThread().getName());
    }
}
