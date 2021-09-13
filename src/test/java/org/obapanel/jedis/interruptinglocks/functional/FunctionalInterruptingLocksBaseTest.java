package org.obapanel.jedis.interruptinglocks.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.common.test.JedisTestFactory;
import org.obapanel.jedis.interruptinglocks.InterruptingJedisJedisLockBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.obapanel.jedis.interruptinglocks.functional.JedisTestFactoryLocks.checkLock;


public class FunctionalInterruptingLocksBaseTest {

    private static final Logger log = LoggerFactory.getLogger(FunctionalInterruptingLocksBaseTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private String lockName;
    private JedisPool jedisPool;

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        lockName = "flock:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        jedisPool = jtfTest.createJedisPool();

    }

    @After
    public void after() {
        if (jedisPool != null) jedisPool.close();
    }


    @Test
    public void testIfInterruptedFor5SecondsLock() {
        try {
            for(int i = 0; i < jtfTest.getFunctionalTestCycles(); i++) {
                log.info("_\n");
                log.info("FUNCTIONAL_TEST_CYCLES " + i);
                log.info("_i" + i + "_3s");
                boolean wasInterruptedFor3Seconds = wasInterrupted(3);
                log.info("_i" + i + "_7s");
                boolean wasInterruptedFor7Seconds = wasInterrupted(7);
                log.info("_i" + i + "_1s");
                boolean wasInterruptedFor1Seconds = wasInterrupted(1);
                log.info("_i" + i + "_9s");
                boolean wasInterruptedFor9Seconds = wasInterrupted(9);
                assertFalse(wasInterruptedFor3Seconds);
                assertTrue(wasInterruptedFor7Seconds);
                assertFalse(wasInterruptedFor1Seconds);
                assertTrue(wasInterruptedFor9Seconds);
            }
        } catch (Exception e) {
            log.error("Error while locking", e);
            fail();
        }
    }


    private boolean wasInterrupted(int sleepSeconds){
        boolean wasInterrupted = false;
        Jedis jedis = jtfTest.createJedisClient();
        InterruptingJedisJedisLockBase interruptingJedisJedisLockBase = new InterruptingJedisJedisLockBase(jedisPool, lockName, 5, TimeUnit.SECONDS);
        interruptingJedisJedisLockBase.lock();
        checkLock(interruptingJedisJedisLockBase);
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(sleepSeconds));
        } catch (InterruptedException e) {
            wasInterrupted = true;
        }
        interruptingJedisJedisLockBase.unlock();
        jedis.close();
        return wasInterrupted;
    }
}
