package org.oba.jedis.extra.utils.interruptinglocks.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.interruptinglocks.InterruptingJedisJedisLockExecutor;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FunctionalInterruptingLocksWithExecutorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalInterruptingLocksWithExecutorTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private String lockName;
    private JedisPool jedisPool;
    private ExecutorService executorService;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        executorService = Executors.newFixedThreadPool(4);
        jedisPool = jtfTest.createJedisPool();
        lockName = "lock_" + this.getClass().getName() + "_" + System.currentTimeMillis();
    }

    @After
    public void after() {
        if (!jtfTest.functionalTestEnabled()) return;
        if (jedisPool != null) jedisPool.close();
        if (executorService != null) executorService.shutdown();
    }


    @Test
    public void testIfInterruptedFor5SecondsLock() {
        for(int i = 0; i < jtfTest.getFunctionalTestCycles(); i++) {
            LOGGER.info("_\n");
            LOGGER.info("i {}", i);
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
        JedisTestFactoryLocks.checkLock(interruptingLock);
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(sleepSeconds));
        } catch (InterruptedException e) {
            wasInterrupted = true;
        }
        interruptingLock.unlock();
        return wasInterrupted;
    }
}
