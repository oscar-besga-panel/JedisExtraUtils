package org.oba.jedis.extra.utils.interruptinglocks;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
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
        JedisLockUtils.underLockTask(mockOfJedis.getJedisPooled(), lockName,() ->
            result1.set(true)
        );
        boolean result2 = JedisLockUtils.underLockTask(mockOfJedis.getJedisPooled(), lockName,() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test
    public void underLockTaskSc() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        JedisLockUtils.underLockTask(mockOfJedis.getJedisPooled(), lockName,() ->
                result1.set(true)
        );
        boolean result2 = JedisLockUtils.underLockTask(mockOfJedis.getJedisPooled(), lockName,() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }


    @Test
    public void underLock() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        JedisLock jedisLock1 = new JedisLock(mockOfJedis.getJedisPooled(), lockName);
        jedisLock1.underLock(() -> result1.set(true));
        JedisLock jedisLock2 = new JedisLock(mockOfJedis.getJedisPooled(), lockName);
        boolean result2 = jedisLock2.underLock(() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test
    public void underLockSc() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        JedisLock jedisLock1 = new JedisLock(mockOfJedis.getJedisPooled(), lockName);
        jedisLock1.underLock(() -> result1.set(true));
        JedisLock jedisLock2 = new JedisLock(mockOfJedis.getJedisPooled(), lockName);
        boolean result2 = jedisLock2.underLock(() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test
    public void underLockWithInterrupted() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        InterruptingJedisJedisLockBase jedisLock1 = new InterruptingJedisJedisLockBase(mockOfJedis.getJedisPooled(), lockName, 1, TimeUnit.SECONDS);
        jedisLock1.underLock(() -> result1.set(true) );
        InterruptingJedisJedisLockBase jedisLock2 = new InterruptingJedisJedisLockBase(mockOfJedis.getJedisPooled(), lockName, 1, TimeUnit.SECONDS);
        boolean result2 = jedisLock2.underLock(() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }
}
