package org.oba.jedis.extra.utils.rateLimiter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisPool;

import java.io.IOException;

import static org.junit.Assert.*;

public class ThrottlingRateLimiterTest {


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
    public void createTest() {
        ThrottlingRateLimiter throttlingRateLimiter = new ThrottlingRateLimiter(jedisPool, rateLimiterName).
                create(1000);
        assertTrue(throttlingRateLimiter.exists());
        assertEquals("1000000", mockOfJedis.getData(rateLimiterName).get(ThrottlingRateLimiter.ALLOW_MICROS));
        assertNotNull(mockOfJedis.getData(rateLimiterName).get(ThrottlingRateLimiter.LAST_ALLOW_MICROS));
    }

}
