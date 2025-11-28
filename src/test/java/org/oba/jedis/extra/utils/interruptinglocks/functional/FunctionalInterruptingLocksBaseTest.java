package org.oba.jedis.extra.utils.interruptinglocks.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.interruptinglocks.InterruptingJedisJedisLockBase;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.oba.jedis.extra.utils.test.WithJedisPoolDelete;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPooled;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class FunctionalInterruptingLocksBaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalInterruptingLocksBaseTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private String lockName;
    private JedisPooled jedisPooled;

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        lockName = "flock:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        jedisPooled = jtfTest.createJedisPooled();

    }

    @After
    public void after() {
        if (jedisPooled != null) {
            jedisPooled.del(lockName);
            jedisPooled.close();
        }
    }


    @Test
    public void testIfInterruptedFor5SecondsLock() {
        try {
            for(int i = 0; i < jtfTest.getFunctionalTestCycles(); i++) {
                LOGGER.info("_\n");
                LOGGER.info("FUNCTIONAL_TEST_CYCLES " + i);
                LOGGER.info("_i" + i + "_3s");
                boolean wasInterruptedFor3Seconds = wasInterrupted(3);
                LOGGER.info("_i" + i + "_7s");
                boolean wasInterruptedFor7Seconds = wasInterrupted(7);
                LOGGER.info("_i" + i + "_1s");
                boolean wasInterruptedFor1Seconds = wasInterrupted(1);
                LOGGER.info("_i" + i + "_9s");
                boolean wasInterruptedFor9Seconds = wasInterrupted(9);
                assertFalse(wasInterruptedFor3Seconds);
                assertTrue(wasInterruptedFor7Seconds);
                assertFalse(wasInterruptedFor1Seconds);
                assertTrue(wasInterruptedFor9Seconds);
            }
        } catch (Exception e) {
            LOGGER.error("Error while locking", e);
            fail();
        }
    }


    private boolean wasInterrupted(int sleepSeconds){
        boolean wasInterrupted = false;
        Jedis jedis = jtfTest.createJedisClient();
        InterruptingJedisJedisLockBase interruptingJedisJedisLockBase = new InterruptingJedisJedisLockBase(jedisPooled, lockName, 5, TimeUnit.SECONDS);
        interruptingJedisJedisLockBase.lock();
        JedisTestFactoryLocks.checkLock(interruptingJedisJedisLockBase);
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
