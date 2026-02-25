package org.oba.jedis.extra.utils.utils.functional;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.oba.jedis.extra.utils.utils.JedisPoolToUnifiedRedis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class FunctionalJedisPoolToUnifiedRedisTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalJedisPoolToUnifiedRedisTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();


    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
    }

    @After
    public void after() throws IOException {
        if (!jtfTest.functionalTestEnabled()) return;
    }

    @Test
    public void asUnifiedJedisTest() {
        HostAndPort hostAndPort = new HostAndPort(jtfTest.getHost(), jtfTest.getPort());
        JedisClientConfig jedisClientConfig = DefaultJedisClientConfig.builder().build();
        JedisPool jedisPool = new JedisPool(hostAndPort, jedisClientConfig);
        LOGGER.debug("jedis pool create to hostAndPort {}", hostAndPort);
        UnifiedJedis redisClient = JedisPoolToUnifiedRedis.asUnifiedJedis(jedisPool);
        String result = redisClient.ping();
        jedisPool.close();
        LOGGER.debug("result {}", result);
        assertTrue(result != null && !result.isEmpty());
    }


}
