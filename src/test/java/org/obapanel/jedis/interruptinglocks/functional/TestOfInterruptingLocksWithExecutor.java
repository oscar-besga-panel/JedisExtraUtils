package org.obapanel.jedis.interruptinglocks.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.interruptinglocks.InterruptingJedisJedisLockExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.interruptinglocks.functional.JedisTestFactory.TEST_CYCLES;
import static org.obapanel.jedis.interruptinglocks.functional.JedisTestFactory.checkLock;
import static org.obapanel.jedis.interruptinglocks.functional.JedisTestFactory.functionalTestEnabled;

public class TestOfInterruptingLocksWithExecutor {

    private static final Logger log = LoggerFactory.getLogger(TestOfInterruptingLocksWithExecutor.class);




    private String lockName;
    private Jedis jedis;
    private ExecutorService executorService;



    @Before
    public void before() {
        org.junit.Assume.assumeTrue(functionalTestEnabled());
        if (TEST_CYCLES <= 0) return;
        executorService = Executors.newFixedThreadPool(1);
        jedis = JedisTestFactory.createJedisClient();
        lockName = "lock_" + this.getClass().getName() + "_" + System.currentTimeMillis();
    }

    @After
    public void after() {
        if (TEST_CYCLES <= 0) return;
        jedis.quit();
    }


    @Test
    public void testIfInterruptedFor5SecondsLock() {
        for(int i=0; i < TEST_CYCLES ; i++) {
            log.info("i {}", i);
            boolean wasInterruptedFor3Seconds = wasInterrupted(3);
            boolean wasInterruptedFor7Seconds = wasInterrupted(7);
            boolean wasInterruptedFor1Seconds = wasInterrupted(1);
            boolean wasInterruptedFor9Seconds = wasInterrupted(9);

            assertFalse(wasInterruptedFor3Seconds);
            assertTrue(wasInterruptedFor7Seconds);
            assertFalse(wasInterruptedFor1Seconds);
            assertTrue(wasInterruptedFor9Seconds);
        }

    }


    private boolean wasInterrupted(int sleepSeconds){
        boolean wasInterrupted = false;
//            public z_InterruptingJedisJedisLockExecutor(Jedis jedis, String name, long leaseTime, TimeUnit timeUnit, ExecutorService executorService) {

        InterruptingJedisJedisLockExecutor interruptingLock = new InterruptingJedisJedisLockExecutor(jedis, lockName, 5, TimeUnit.SECONDS, executorService);
        interruptingLock.lock();
        checkLock(interruptingLock);
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(sleepSeconds));
        } catch (InterruptedException e) {
            wasInterrupted = true;
        }
        interruptingLock.unlock();
        return wasInterrupted;
    }
}
