package org.oba.jedis.extra.utils.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.utils.functional.FunctionalRedisTimeTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.oba.jedis.extra.utils.utils.MockOfJedis.unitTestEnabled;

public class RedisTimeTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisTimeTest.class);


    private MockOfJedis mockOfJedis;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(unitTestEnabled());
        if (!unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
    }

    @After
    public void after() throws IOException {
        mockOfJedis.getJedisPooled().close();
        mockOfJedis.clearData();
    }

    @Test
    public void timeTest() {
        RedisTime redisTime = new RedisTime(mockOfJedis.getJedisPooled());
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

    @Test
    public void timeTestWithData() {
        mockOfJedis.setGivenTimestamp(123456789012345L);
        RedisTime redisTime = new RedisTime(mockOfJedis.getJedisPooled());
        List<String> timeFromRedis = redisTime.callTime();
        LOGGER.debug("Time from Redis: {}", timeFromRedis);
        BigInteger seconds = redisTime.callTimeInSeconds();
        BigInteger millis = redisTime.callTimeInMillis();
        BigInteger micros = redisTime.callTimeInMicros();
        BigInteger nanos = redisTime.callTimeInNanos();
        LOGGER.debug("seconds: {}, millis: {}, micros: {}, nanos: {}", seconds, millis, micros, nanos);
        assertNotNull(timeFromRedis);
        assertEquals(2, timeFromRedis.size());
        assertEquals("123456", timeFromRedis.get(0));
        assertEquals("789012", timeFromRedis.get(1));
        assertNotNull(seconds);
        assertEquals(123456L, seconds.longValue());
        assertNotNull(millis);
        assertEquals(123456789L, millis.longValue());
        assertNotNull(micros);
        assertEquals(123456789012L, micros.longValue());
        assertNotNull(nanos);
        assertEquals(123456789012000L, nanos.longValue());
    }

}
