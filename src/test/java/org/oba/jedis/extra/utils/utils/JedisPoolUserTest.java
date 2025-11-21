package org.oba.jedis.extra.utils.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class JedisPoolUserTest implements JedisPoolUser {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisPoolUserTest.class);

    private MockOfJedis mockOfJedis;
    private JedisPool jedisPool;
    private Jedis jedis;


    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(MockOfJedis.unitTestEnabled());
        if (!MockOfJedis.unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
        jedisPool = mockOfJedis.getJedisPool();
        jedis = mockOfJedis.getJedis();
    }

    @After
    public void after() throws IOException {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del("a");
            jedis.del("b");
            jedis.del("c");
        }
        jedisPool.close();
        jedis.close();
        mockOfJedis.clearData();
    }


    @Override
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    @Test
    public void testJedisDo() {
        withResource(jedis -> {
            jedis.set("a","1");
            jedis.set("b","2");
            jedis.set("c","3");
        });
        withResource( jedis -> assertNotNull(jedis.ping()));
        assertTrue(jedis.exists("a"));
        assertTrue(jedis.exists("b"));
        assertTrue(jedis.exists("c"));
    }

    @Test
    public void testJedisGet() {
        boolean result = withResourceGet(jedis -> {
            jedis.set("a","1");
            jedis.set("b","2");
            jedis.set("c","3");
            return jedis.exists("a") && jedis.exists("b") && jedis.exists("c");
        });
        String ping = withResourceGet(Jedis::ping);
        assertNotNull(ping);
        assertTrue(result);
        assertTrue(jedis.exists("a"));
        assertTrue(jedis.exists("b"));
        assertTrue(jedis.exists("c"));
    }

}
