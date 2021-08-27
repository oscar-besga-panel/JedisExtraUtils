package org.obapanel.jedis.interruptinglocks.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.interruptinglocks.JedisLock;
import org.obapanel.jedis.utils.JedisPoolAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.obapanel.jedis.interruptinglocks.functional.JedisTestFactory.*;


public class FunctionalJedisLocksScOnCriticalZoneTest {

    private static final Logger log = LoggerFactory.getLogger(FunctionalJedisLocksScOnCriticalZoneTest.class);

    private final AtomicBoolean intoCriticalZone = new AtomicBoolean(false);
    private final AtomicBoolean errorInCriticalZone = new AtomicBoolean(false);
    private final AtomicBoolean otherError = new AtomicBoolean(false);

    private final List<Jedis> jedisList = new ArrayList<>();
    private final List<JedisPool> jedisPoolList = new ArrayList<>();
    private String lockName;
    private final List<JedisLock> lockList = new ArrayList<>();




    @Before
    public void before() {
        org.junit.Assume.assumeTrue(functionalTestEnabled());
        if (!functionalTestEnabled()) return;
        lockName = "flock:" + this.getClass().getName() + ":" + System.currentTimeMillis();
    }

    @After
    public void after() {
        if (!functionalTestEnabled()) return;
        lockList.stream().
                filter(Objects::nonNull).
                forEach(il -> {
                    if (il.isLocked()) {
                        log.error("A lock named {} is locked !", il.getName());
                    }
                    il.unlock();
        });
        jedisPoolList.forEach(JedisPool::close);
        jedisList.forEach(Jedis::close);
    }

    JedisPool createJedisPoolAdapter() {
        Jedis jedis = createJedisClient();
        jedisList.add(jedis);
        JedisPool jedisPool = JedisPoolAdapter.poolFromJedis(jedis);
        jedisPoolList.add(jedisPool);
        return jedisPool;
    }


    @Test
    public void testIfInterruptedFor5SecondsLock() throws InterruptedException {
        for(int i = 0; i < FUNCTIONAL_TEST_CYCLES; i++) {
            intoCriticalZone.set(false);
            errorInCriticalZone.set(false);
            otherError.set(false);
            log.info("_\n");
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
            JedisPool jedisPool = createJedisPoolAdapter();
            JedisLock jedisLock = new JedisLock(jedisPool, lockName);
            lockList.add(jedisLock);
            jedisLock.lock();
            checkLock(jedisLock);
            accessCriticalZone(sleepTime);
            jedisLock.unlock();
        } catch (Exception e){
            log.error("Error ", e);
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
            // NOPE
        }
        intoCriticalZone.set(false);
    }
}
