package org.oba.jedis.extra.utils.rateLimiter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class BucketRateLimiterTest {

    private MockOfJedis mockOfJedis;
    private JedisPool jedisPool;
    private String rateLimiterName;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(MockOfJedis.unitTestEnabled());
        if (!MockOfJedis.unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
        jedisPool = mockOfJedis.getJedisPool();
        rateLimiterName = "rateLimiterName:" + this.getClass().getName() + ":" + System.currentTimeMillis();
    }

    @After
    public void after() throws IOException {
        jedisPool.close();
        mockOfJedis.clearData();
    }

    @Test
    public void create1Test() {
        BucketRateLimiter bucketRateLimiter = new BucketRateLimiter(jedisPool, rateLimiterName).
                create(10, BucketRateLimiter.Mode.INTERVAL, 1 , TimeUnit.SECONDS);
        assertTrue(bucketRateLimiter.exists());
        assertEquals("1000000", mockOfJedis.getData(rateLimiterName).get(BucketRateLimiter.REFILL_MICROS));
        assertEquals("10", mockOfJedis.getData(rateLimiterName).get(BucketRateLimiter.AVAILABLE));
        assertEquals("10", mockOfJedis.getData(rateLimiterName).get(BucketRateLimiter.CAPACITY));
        assertEquals(BucketRateLimiter.Mode.INTERVAL.name().toLowerCase(),
                mockOfJedis.getData(rateLimiterName).get(BucketRateLimiter.MODE));
        assertNotNull(mockOfJedis.getData(rateLimiterName).get(BucketRateLimiter.LAST_REFILL_MICROS));
    }

    @Test
    public void create2Test() {
        BucketRateLimiter bucketRateLimiter = new BucketRateLimiter(jedisPool, rateLimiterName).
                create(5, BucketRateLimiter.Mode.GREEDY, 2 , TimeUnit.SECONDS);
        assertTrue(bucketRateLimiter.exists());
        assertEquals("2000000", mockOfJedis.getData(rateLimiterName).get(BucketRateLimiter.REFILL_MICROS));
        assertEquals("5", mockOfJedis.getData(rateLimiterName).get(BucketRateLimiter.AVAILABLE));
        assertEquals("5", mockOfJedis.getData(rateLimiterName).get(BucketRateLimiter.CAPACITY));
        assertEquals(BucketRateLimiter.Mode.GREEDY.name().toLowerCase(),
                mockOfJedis.getData(rateLimiterName).get(BucketRateLimiter.MODE));
        assertNotNull(mockOfJedis.getData(rateLimiterName).get(BucketRateLimiter.LAST_REFILL_MICROS));
    }

}
