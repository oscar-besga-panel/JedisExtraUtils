package org.oba.jedis.extra.utils.rateLimiter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
    public void create0Test() {
        ThrottlingRateLimiter throttlingRateLimiter = new ThrottlingRateLimiter(jedisPool, rateLimiterName).
                create(1, TimeUnit.SECONDS);
        assertTrue(throttlingRateLimiter.exists());
        throttlingRateLimiter.delete();
        assertFalse(throttlingRateLimiter.exists());
        throttlingRateLimiter.
                createIfNotExists(1500);
        assertTrue(throttlingRateLimiter.exists());
        throttlingRateLimiter.
                createIfNotExists(2, TimeUnit.SECONDS);
        assertTrue(throttlingRateLimiter.exists());
        assertEquals("1500000", mockOfJedis.getData(rateLimiterName).get("allow_micros"));
    }

    @Test
    public void create1Test() {
        ThrottlingRateLimiter throttlingRateLimiter = new ThrottlingRateLimiter(jedisPool, rateLimiterName).
                create(1000);
        assertTrue(throttlingRateLimiter.exists());
        assertEquals("1000000", mockOfJedis.getData(rateLimiterName).get(ThrottlingRateLimiter.ALLOW_MICROS));
        assertNotNull(mockOfJedis.getData(rateLimiterName).get(ThrottlingRateLimiter.LAST_ALLOW_MICROS));
        throttlingRateLimiter.delete();
        assertFalse(throttlingRateLimiter.exists());
    }

    @Test
    public void allowTest() {
        AtomicReference<String> passedName = new AtomicReference<>("");
        AtomicInteger num = new AtomicInteger(1);
        mockOfJedis.setDoWithEvalSha((t,u,v) -> {
            passedName.set(u.get(0));
            return Boolean.valueOf(num.incrementAndGet() % 2 == 0);
        });
        ThrottlingRateLimiter throttlingRateLimiter = new ThrottlingRateLimiter(jedisPool, rateLimiterName).
                create(1000);
        assertTrue(throttlingRateLimiter.allow());
        assertFalse(throttlingRateLimiter.allow());
        assertEquals(throttlingRateLimiter.getName(), passedName.get());
    }

    //TODO advanced test, but must re-implement lua scripting in java, and now I don't feel like it...

}