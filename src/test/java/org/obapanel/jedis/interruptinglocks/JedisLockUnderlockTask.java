package org.obapanel.jedis.interruptinglocks;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.interruptinglocks.MockOfJedis.integrationTestEnabled;

public class JedisLockUnderlockTask {

    private String lockName;
    private MockOfJedis mockOfJedis;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(integrationTestEnabled());
        if (!integrationTestEnabled()) return;
        lockName = "lock:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        mockOfJedis = new MockOfJedis();
    }

    @After
    public void after() {
        if (mockOfJedis != null) mockOfJedis.clearData();
    }


    @Test
    public void underLockTask() throws Exception {
        AtomicBoolean result1 = new AtomicBoolean(false);
        IJedisLock.underLockTask(mockOfJedis.getJedis(), lockName,() -> {
            result1.set(true);
        });
        boolean result2 = IJedisLock.underLockTask(mockOfJedis.getJedis(), lockName,() -> {
            return true;
        });
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test
    public void underLock() throws Exception {
        AtomicBoolean result1 = new AtomicBoolean(false);
        JedisLock jedisLock1 = new JedisLock(mockOfJedis.getJedis(), lockName);
        jedisLock1.underLock(() -> {
            result1.set(true);
        });
        JedisLock jedisLock2 = new JedisLock(mockOfJedis.getJedis(), lockName);
        boolean result2 = jedisLock2.underLock(() -> {
            return true;
        });
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test
    public void underLockWithInterrupted() throws Exception {
        AtomicBoolean result1 = new AtomicBoolean(false);
        InterruptingJedisJedisLockBase jedisLock1 = new InterruptingJedisJedisLockBase(mockOfJedis.getJedis(), lockName, 1, TimeUnit.SECONDS);
        jedisLock1.underLock(() -> {
            result1.set(true);
        });
        InterruptingJedisJedisLockBase jedisLock2 = new InterruptingJedisJedisLockBase(mockOfJedis.getJedis(), lockName, 1, TimeUnit.SECONDS);
        boolean result2 = jedisLock2.underLock(() -> {
            return true;
        });
        assertTrue(result1.get());
        assertTrue(result2);
    }
}
