package org.obapanel.jedis.interruptinglocks.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.interruptinglocks.InterruptingJedisJedisLockBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.interruptinglocks.functional.JedisTestFactory.FUNCTIONAL_TEST_CYCLES;
import static org.obapanel.jedis.interruptinglocks.functional.JedisTestFactory.checkLock;
import static org.obapanel.jedis.interruptinglocks.functional.JedisTestFactory.createJedisPool;
import static org.obapanel.jedis.interruptinglocks.functional.JedisTestFactory.functionalTestEnabled;


public class FunctionalInterruptingLocksTest {

    private static final Logger log = LoggerFactory.getLogger(FunctionalInterruptingLocksTest.class);


    private String lockName;
    private JedisPool jedisPool;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(functionalTestEnabled());
        if (!functionalTestEnabled()) return;
        jedisPool = createJedisPool();
        lockName = "lock:" + this.getClass().getName() + ":" + System.currentTimeMillis();
    }

    @After
    public void after() {
        if (jedisPool != null) jedisPool.close();
    }

    @Test
    public void testIfInterruptedFor5SecondsLock() {
        for(int i = 0; i < FUNCTIONAL_TEST_CYCLES; i++) {
            log.info("_\n");
            log.info("i {}", i);
            boolean wasInterruptedFor3Seconds = wasInterrupted(3);
            log.info("i {} wasInterruptedFor3Seconds {}", i, wasInterruptedFor3Seconds);
            boolean wasInterruptedFor7Seconds = wasInterrupted(7);
            log.info("i {} wasInterruptedFor7Seconds {}", i, wasInterruptedFor7Seconds);
            boolean wasInterruptedFor1Seconds = wasInterrupted(1);
            log.info("i {} wasInterruptedFor1Seconds {}", i, wasInterruptedFor1Seconds);
            boolean wasInterruptedFor9Seconds = wasInterrupted(9);
            log.info("i {} wasInterruptedFor9Seconds {}", i, wasInterruptedFor9Seconds);
            assertFalse(wasInterruptedFor3Seconds);
            assertTrue(wasInterruptedFor7Seconds);
            assertFalse(wasInterruptedFor1Seconds);
            assertTrue(wasInterruptedFor9Seconds);
        }

    }


    private boolean wasInterrupted(int sleepSeconds){
        boolean wasInterrupted = false;
        boolean wasLocked = false;
        InterruptingJedisJedisLockBase interruptingJedisLock = new InterruptingJedisJedisLockBase(jedisPool, lockName, 5, TimeUnit.SECONDS);
        interruptingJedisLock.lock();
        wasLocked = checkLock(interruptingJedisLock);
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(sleepSeconds));
        } catch (InterruptedException e) {
            wasInterrupted = true;
        }
        interruptingJedisLock.unlock();
        log.info("thread wasLocked " + wasLocked + " wasInterrupted " + wasInterrupted + " thread " + Thread.currentThread().getName());
        return wasInterrupted && wasLocked;
    }
}
