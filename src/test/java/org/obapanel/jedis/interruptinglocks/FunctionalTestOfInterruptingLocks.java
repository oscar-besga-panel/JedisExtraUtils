package org.obapanel.jedis.interruptinglocks;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class FunctionalTestOfInterruptingLocks {

    private static final Logger log = LoggerFactory.getLogger(FunctionalTestOfInterruptingLocks.class);


    private String lockName;
    private Jedis jedis;


    @Before
    public void before() {
        jedis = JedisTestFactory.createJedisClient();
        lockName = "lock_" + this.getClass().getName() + "_" + System.currentTimeMillis();
    }

    @After
    public void after() {
        jedis.quit();
    }

    @Test
    public void testIfInterruptedFor5SecondsLock() {
        for(int i=0; i< 3; i++) {
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
        InterruptingJedisJedisLock interruptingJedisLock = new InterruptingJedisJedisLock(jedis,lockName, 5, TimeUnit.SECONDS);
        interruptingJedisLock.lock();
        if (interruptingJedisLock.isLocked()) {
            wasLocked = true;
        } else {
            log.error("wasInterruptedForXSeconds {} not locked",sleepSeconds);
            throw new IllegalStateException("wasInterruptedForXSeconds " +  sleepSeconds + "not locked");        }
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(sleepSeconds));
        } catch (InterruptedException e) {
            wasInterrupted = true;
        }
        interruptingJedisLock.unlock();
        return wasInterrupted && wasLocked;
    }
}
