package org.oba.jedis.extra.utils.utils.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.oba.jedis.extra.utils.utils.RedisTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class FunctionalRedisTimeTest {


    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalRedisTimeTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private JedisPooled jedisPooled;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPooled = jtfTest.createJedisPooled();
    }

    @After
    public void after() throws IOException {
        if (!jtfTest.functionalTestEnabled()) return;
        if (jedisPooled != null) {
            jedisPooled.close();
        }
    }

    @Test
    public void timeTest() {
        RedisTime redisTime = new RedisTime(jedisPooled);
        List<String> timeFromRedis = redisTime.callTime();
        LOGGER.debug("Time from Redis: {}", timeFromRedis);
        BigInteger seconds = redisTime.callTimeInSeconds();
        BigInteger millis = redisTime.callTimeInMillis();
        BigInteger micros = redisTime.callTimeInMicros();
        BigInteger nanos = redisTime.callTimeInNanos();
        LOGGER.debug("seconds: {}, millis: {}, micros: {}, nanos: {}", seconds, millis, micros, nanos);
        assertNotNull(timeFromRedis);
        assertEquals(2, timeFromRedis.size());
        assertNotNull(seconds);
        assertNotNull(millis);
        assertNotNull(micros);
        assertNotNull(nanos);
    }

}
