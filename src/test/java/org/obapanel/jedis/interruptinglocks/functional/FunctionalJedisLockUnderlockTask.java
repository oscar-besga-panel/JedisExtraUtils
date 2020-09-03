package org.obapanel.jedis.interruptinglocks.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.interruptinglocks.InterruptingJedisJedisLockBase;
import org.obapanel.jedis.interruptinglocks.JedisLock;
import org.obapanel.jedis.interruptinglocks.JedisLockUtils;
import redis.clients.jedis.Jedis;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.interruptinglocks.functional.JedisTestFactory.functionalTestEnabled;

public class FunctionalJedisLockUnderlockTask {

    private Jedis jedis;
    private String keyName;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(functionalTestEnabled());
        if (!functionalTestEnabled()) return;
        jedis = JedisTestFactory.createJedisClient();
        keyName = "lock:" + this.getClass().getName() + ":" + System.currentTimeMillis();
    }


    @After
    public void tearDown() {
        if (jedis != null ) {
            jedis.del(keyName);
            jedis.quit();
        }
    }


    @Test
    public void underLockTask() throws Exception {
        AtomicBoolean result1 = new AtomicBoolean(false);
        JedisLockUtils.underLockTask(jedis, keyName, () -> {
            result1.set(true);
        });
        boolean result2 = JedisLockUtils.underLockTask(jedis, keyName, () -> {
            return true;
        });
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test
    public void underLock() throws Exception {
        AtomicBoolean result1 = new AtomicBoolean(false);
        JedisLock jedisLock1 = new JedisLock(jedis, keyName);
        jedisLock1.underLock(() -> {
            result1.set(true);
        });
        JedisLock jedisLock2 = new JedisLock(jedis, keyName);
        boolean result2 = jedisLock2.underLock(() -> {
            return true;
        });
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test
    public void underLockWithInterrupted() throws Exception {
        AtomicBoolean result1 = new AtomicBoolean(false);
        InterruptingJedisJedisLockBase jedisLock1 = new InterruptingJedisJedisLockBase(jedis, keyName, 1, TimeUnit.SECONDS);
        jedisLock1.underLock(() -> {
            result1.set(true);
        });
        InterruptingJedisJedisLockBase jedisLock2 = new InterruptingJedisJedisLockBase(jedis, keyName, 1, TimeUnit.SECONDS);
        boolean result2 = jedisLock2.underLock(() -> {
            return true;
        });
        assertTrue(result1.get());
        assertTrue(result2);
    }

}
