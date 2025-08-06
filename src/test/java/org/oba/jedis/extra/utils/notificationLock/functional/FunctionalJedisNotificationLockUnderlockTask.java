package org.oba.jedis.extra.utils.notificationLock.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.notificationLock.NotificationLock;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.oba.jedis.extra.utils.test.WithJedisPoolDelete;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

public class FunctionalJedisNotificationLockUnderlockTask {

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private JedisPool jedisPool;
    private String keyName;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPool = jtfTest.createJedisPool();
        keyName = "lock:" + this.getClass().getName() + ":" + System.currentTimeMillis();
    }


    @After
    public void tearDown() {
        if (!jtfTest.functionalTestEnabled()) return;
        if (jedisPool != null) {
            WithJedisPoolDelete.doDelete(jedisPool, keyName);
            jedisPool.close();
        }
    }


    @Test
    public void underLock() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        NotificationLock jedisLock1 = new NotificationLock(jedisPool, keyName);
        jedisLock1.underLock(() -> result1.set(true) );
        NotificationLock jedisLock2 = new NotificationLock(jedisPool, keyName);
        boolean result2 = jedisLock2.underLock(() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }


}
