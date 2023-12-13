package org.oba.jedis.extra.utils.notificationLock;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.oba.jedis.extra.utils.utils.JedisPoolAdapter;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBase;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({Transaction.class, TransactionBase.class })
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
        NotificationLock.underLockTask(mockOfJedis.getJedisPool(), lockName,() ->
                result1.set(true)
        );
        boolean result2 = NotificationLock.underLockTask(mockOfJedis.getJedisPool(), lockName,() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test
    public void underLock() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        NotificationLock jedisLock1 = new NotificationLock(mockOfJedis.getJedisPool(), lockName);
        jedisLock1.underLock(() -> result1.set(true));
        NotificationLock jedisLock2 = new NotificationLock(mockOfJedis.getJedisPool(), lockName);
        boolean result2 = jedisLock2.underLock(() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test
    public void underLockSc() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        NotificationLock jedisLock1 = new NotificationLock(JedisPoolAdapter.poolFromJedis(mockOfJedis.getJedis()), lockName);
        jedisLock1.underLock(() -> result1.set(true));
        NotificationLock jedisLock2 = new NotificationLock(JedisPoolAdapter.poolFromJedis(mockOfJedis.getJedis()), lockName);
        boolean result2 = jedisLock2.underLock(() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

}
