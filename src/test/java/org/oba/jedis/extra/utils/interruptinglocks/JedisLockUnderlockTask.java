package org.oba.jedis.extra.utils.interruptinglocks;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.oba.jedis.extra.utils.utils.JedisPoolAdapter;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import redis.clients.jedis.Transaction;


import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({Transaction.class })
public class JedisLockUnderlockTask {

    private String lockName;
    private MockOfJedis mockOfJedis;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(MockOfJedis.unitTestEnabled());
        if (!MockOfJedis.unitTestEnabled()) return;
        lockName = "lock:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        mockOfJedis = new MockOfJedis();
    }

    @After
    public void after() {
        if (mockOfJedis != null) mockOfJedis.clearData();
    }


    @Test
    public void underLockTask() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        JedisLockUtils.underLockTask(mockOfJedis.getJedisPool(), lockName,() ->
            result1.set(true)
        );
        boolean result2 = JedisLockUtils.underLockTask(mockOfJedis.getJedisPool(), lockName,() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test
    public void underLockTaskSc() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        JedisLockUtils.underLockTask(mockOfJedis.getJedis(), lockName,() ->
                result1.set(true)
        );
        boolean result2 = JedisLockUtils.underLockTask(mockOfJedis.getJedis(), lockName,() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }


    @Test
    public void underLock() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        JedisLock jedisLock1 = new JedisLock(mockOfJedis.getJedisPool(), lockName);
        jedisLock1.underLock(() -> result1.set(true));
        JedisLock jedisLock2 = new JedisLock(mockOfJedis.getJedisPool(), lockName);
        boolean result2 = jedisLock2.underLock(() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test
    public void underLockSc() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        JedisLock jedisLock1 = new JedisLock(JedisPoolAdapter.poolFromJedis(mockOfJedis.getJedis()), lockName);
        jedisLock1.underLock(() -> result1.set(true));
        JedisLock jedisLock2 = new JedisLock(JedisPoolAdapter.poolFromJedis(mockOfJedis.getJedis()), lockName);
        boolean result2 = jedisLock2.underLock(() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test
    public void underLockWithInterrupted() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        InterruptingJedisJedisLockBase jedisLock1 = new InterruptingJedisJedisLockBase(mockOfJedis.getJedisPool(), lockName, 1, TimeUnit.SECONDS);
        jedisLock1.underLock(() -> result1.set(true) );
        InterruptingJedisJedisLockBase jedisLock2 = new InterruptingJedisJedisLockBase(mockOfJedis.getJedisPool(), lockName, 1, TimeUnit.SECONDS);
        boolean result2 = jedisLock2.underLock(() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }
}
