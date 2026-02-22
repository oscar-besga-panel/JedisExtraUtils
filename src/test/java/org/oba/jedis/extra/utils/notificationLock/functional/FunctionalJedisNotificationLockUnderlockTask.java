package org.oba.jedis.extra.utils.notificationLock.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.notificationLock.NotificationLock;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import redis.clients.jedis.UnifiedJedis;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

public class FunctionalJedisNotificationLockUnderlockTask {

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private UnifiedJedis redisClient;
    private String keyName;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        redisClient = jtfTest.createRedisClient();
        keyName = "lock:" + this.getClass().getName() + ":" + System.currentTimeMillis();
    }


    @After
    public void tearDown() {
        if (!jtfTest.functionalTestEnabled()) return;
        if (redisClient != null) {
            redisClient.del(keyName);
            redisClient.close();
        }
    }


    @Test
    public void underLock() {
        AtomicBoolean result1 = new AtomicBoolean(false);
        NotificationLock jedisLock1 = new NotificationLock(redisClient, keyName);
        jedisLock1.underLock(() -> result1.set(true) );
        NotificationLock jedisLock2 = new NotificationLock(redisClient, keyName);
        boolean result2 = jedisLock2.underLock(() -> true);
        assertTrue(result1.get());
        assertTrue(result2);
    }


}
