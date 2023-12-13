package org.oba.jedis.extra.utils.notificationLock.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.oba.jedis.extra.utils.notificationLock.NotificationLock;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBase;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({Transaction.class, TransactionBase.class })
public class FunctionalJedisLockUnderlockTask {

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
        if (!jtfTest.functionalTestEnabled()) return;
        if (jedisPool != null) {
            jedisPool.close();
        }
    }
    @Test
    public void underLockTask() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        NotificationLock.underLockTask(jedisPool, lockName,() ->
                result1.set(true)
        );
        boolean result2 = NotificationLock.underLockTask(jedisPool, lockName,() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }

    @Test
    public void underLock() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        NotificationLock jedisLock1 = new NotificationLock(jedisPool, lockName);
        jedisLock1.underLock(() -> result1.set(true));
        NotificationLock jedisLock2 = new NotificationLock(jedisPool, lockName);
        boolean result2 = jedisLock2.underLock(() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }


}
