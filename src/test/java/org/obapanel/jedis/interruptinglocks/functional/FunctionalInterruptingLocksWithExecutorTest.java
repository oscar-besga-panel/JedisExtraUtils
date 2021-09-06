package org.obapanel.jedis.interruptinglocks.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.interruptinglocks.InterruptingJedisJedisLockExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.interruptinglocks.functional.JedisTestFactory.FUNCTIONAL_TEST_CYCLES;
import static org.obapanel.jedis.interruptinglocks.functional.JedisTestFactory.checkLock;
import static org.obapanel.jedis.interruptinglocks.functional.JedisTestFactory.createJedisPool;
import static org.obapanel.jedis.interruptinglocks.functional.JedisTestFactory.functionalTestEnabled;

public class FunctionalInterruptingLocksWithExecutorTest {

    private static final Logger log = LoggerFactory.getLogger(FunctionalInterruptingLocksWithExecutorTest.class);

    private String lockName;
    private JedisPool jedisPool;
    private ExecutorService executorService;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(functionalTestEnabled());
        if (!functionalTestEnabled()) return;
        executorService = Executors.newFixedThreadPool(4);
        jedisPool = createJedisPool();
        lockName = "lock_" + this.getClass().getName() + "_" + System.currentTimeMillis();
    }

    @After
    public void after() {
        if (!functionalTestEnabled()) return;
        if (jedisPool != null) jedisPool.close();
        if (executorService != null) executorService.shutdown();
    }


    @Test
    public void testIfInterruptedFor5SecondsLock() {
        for(int i = 0; i < FUNCTIONAL_TEST_CYCLES; i++) {
            log.info("_\n");
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
        InterruptingJedisJedisLockExecutor interruptingLock = new InterruptingJedisJedisLockExecutor(jedisPool, lockName, 5, TimeUnit.SECONDS, executorService);
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
