package org.obapanel.jedis.interruptinglocks;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FunctionalTestOfInterruptingLocksWithExecutor {

    private static final Logger log = LoggerFactory.getLogger(FunctionalTestOfInterruptingLocksWithExecutor.class);




    private String lockName;
    private Jedis jedis;
    private ExecutorService executorService;



    @Before
    public void before() {
        executorService = Executors.newFixedThreadPool(1);
        jedis = JedisTestFactory.createJedisClient();
        lockName = "lock_" + this.getClass().getName() + "_" + System.currentTimeMillis();
    }

    @After
    public void after() {
        jedis.quit();
    }


    @Test
    public void testIfInterruptedFor5SecondsLock() {

        boolean wasInterruptedFor3Seconds = wasInterrupted(3);
        boolean wasInterruptedFor7Seconds = wasInterrupted(7);
        boolean wasInterruptedFor1Seconds = wasInterrupted(1);
        boolean wasInterruptedFor9Seconds = wasInterrupted(9);

        assertFalse(wasInterruptedFor3Seconds);
        assertTrue(wasInterruptedFor7Seconds);
        assertFalse(wasInterruptedFor1Seconds);
        assertTrue(wasInterruptedFor9Seconds);

    }


    private boolean wasInterrupted(int sleepSeconds){
        boolean wasInterrupted = false;
//            public z_InterruptingJedisJedisLockExecutor(Jedis jedis, String name, long leaseTime, TimeUnit timeUnit, ExecutorService executorService) {

        InterruptingJedisJedisLockExecutor interruptingLock = new InterruptingJedisJedisLockExecutor(jedis, lockName, 5, TimeUnit.SECONDS, executorService);
        interruptingLock.lock();
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(sleepSeconds));
        } catch (InterruptedException e) {
            wasInterrupted = true;
        }
        interruptingLock.unlock();
        return wasInterrupted;
    }
}
