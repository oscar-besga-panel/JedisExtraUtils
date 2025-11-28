package org.oba.jedis.extra.utils.interruptinglocks.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.interruptinglocks.InterruptingJedisJedisLockBase;
import org.oba.jedis.extra.utils.interruptinglocks.JedisLock;
import org.oba.jedis.extra.utils.interruptinglocks.JedisLockUtils;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import redis.clients.jedis.JedisPooled;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

public class FunctionalJedisLockUnderlockTask {

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private JedisPooled jedisPooled;
    private String keyName;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPooled = jtfTest.createJedisPooled();
        keyName = "lock:" + this.getClass().getName() + ":" + System.currentTimeMillis();
    }


    @After
    public void tearDown() {
        if (!jtfTest.functionalTestEnabled()) return;
        if (jedisPooled != null) {
            jedisPooled.del(keyName);
            jedisPooled.close();
        }
    }


    @Test
    public void underLockTask() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        JedisLockUtils.underLockTask(jedisPooled, keyName, () -> result1.set(true) );
        boolean result2 = JedisLockUtils.underLockTask(jedisPooled, keyName, () -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test
    public void underLockTaskSc() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        JedisLockUtils.underLockTask(jedisPooled, keyName, () -> result1.set(true) );
        AtomicBoolean result2 = new AtomicBoolean(false);
        boolean result2tmp = JedisLockUtils.underLockTask(jedisPooled, keyName, () -> true);
        result2.set(result2tmp);
        assertTrue(result1.get());
        assertTrue(result2.get());
    }

    @Test
    public void underLock() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        JedisLock jedisLock1 = new JedisLock(jedisPooled, keyName);
        jedisLock1.underLock(() -> result1.set(true) );
        JedisLock jedisLock2 = new JedisLock(jedisPooled, keyName);
        boolean result2 = jedisLock2.underLock(() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test
    public void underLockWithInterrupted() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        InterruptingJedisJedisLockBase jedisLock1 = new InterruptingJedisJedisLockBase(jedisPooled, keyName, 1, TimeUnit.SECONDS);
        jedisLock1.underLock(() -> result1.set(true));
        InterruptingJedisJedisLockBase jedisLock2 = new InterruptingJedisJedisLockBase(jedisPooled, keyName, 1, TimeUnit.SECONDS);
        boolean result2 = jedisLock2.underLock(() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

}
