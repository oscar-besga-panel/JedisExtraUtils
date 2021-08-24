package org.obapanel.jedis.interruptinglocks.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.interruptinglocks.InterruptingJedisJedisLockBase;
import org.obapanel.jedis.interruptinglocks.JedisLock;
import org.obapanel.jedis.interruptinglocks.JedisLockUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.interruptinglocks.functional.JedisTestFactory.functionalTestEnabled;

public class FunctionalJedisLockUnderlockTask {

    private JedisPool jedisPool;
    private String keyName;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(functionalTestEnabled());
        if (!functionalTestEnabled()) return;
        jedisPool = JedisTestFactory.createJedisPool();
        keyName = "lock:" + this.getClass().getName() + ":" + System.currentTimeMillis();
    }


    @After
    public void tearDown() {
        if (jedisPool != null) {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.del(keyName);
            }
            jedisPool.close();
        }
    }


    @Test
    public void underLockTask() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        JedisLockUtils.underLockTask(jedisPool, keyName, () -> result1.set(true) );
        boolean result2 = JedisLockUtils.underLockTask(jedisPool, keyName, () -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test
    public void underLockTaskSc() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        try (Jedis jedis1 = jedisPool.getResource()) {
            JedisLockUtils.underLockTask(jedis1, keyName, () -> result1.set(true) );
        }
        AtomicBoolean result2 = new AtomicBoolean(false);
        try (Jedis jedis2 = jedisPool.getResource()) {
            boolean result2tmp = JedisLockUtils.underLockTask(jedis2, keyName, () -> true);
            result2.set(result2tmp);
        }
        assertTrue(result1.get());
        assertTrue(result2.get());
    }

    @Test
    public void underLock() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        JedisLock jedisLock1 = new JedisLock(jedisPool, keyName);
        jedisLock1.underLock(() -> result1.set(true) );
        JedisLock jedisLock2 = new JedisLock(jedisPool, keyName);
        boolean result2 = jedisLock2.underLock(() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test
    public void underLockWithInterrupted() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        InterruptingJedisJedisLockBase jedisLock1 = new InterruptingJedisJedisLockBase(jedisPool, keyName, 1, TimeUnit.SECONDS);
        jedisLock1.underLock(() -> result1.set(true));
        InterruptingJedisJedisLockBase jedisLock2 = new InterruptingJedisJedisLockBase(jedisPool, keyName, 1, TimeUnit.SECONDS);
        boolean result2 = jedisLock2.underLock(() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

}
