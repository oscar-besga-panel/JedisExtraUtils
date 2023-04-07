package org.oba.jedis.extra.utils.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JedisPoolAdapterTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisPoolAdapterTest.class);

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

    @Test
    public void testProxy1() {
        JedisPool jedisPoolButReallyIsAProxy = JedisPoolAdapter.poolFromJedis(jedis);
        try(Jedis jedisProxyConnection = jedisPoolButReallyIsAProxy.getResource()){
            jedisProxyConnection.set("a","1");
            jedisProxyConnection.set("b","2");
            jedisProxyConnection.set("c","3");
        }
        assertTrue(jedis.exists("a"));
        assertTrue(jedis.exists("b"));
        assertTrue(jedis.exists("c"));
    }

    @Test
    public void testProxy2() {
        JedisPoolAdapter.poolFromJedis(jedis).withResource( jedisProxyConnection ->{
            jedisProxyConnection.set("a","1");
            jedisProxyConnection.set("b","2");
            jedisProxyConnection.set("c","3");
        });
        assertTrue(jedis.exists("a"));
        assertTrue(jedis.exists("b"));
        assertTrue(jedis.exists("c"));
    }

    @Test
    public void testProxy3() {
        jedis.set("a","1");
        String result = JedisPoolAdapter.poolFromJedis(jedis).
                withResourceFunction( jedisProxyConnection -> {
            return jedisProxyConnection.get("a");
        });
        assertEquals("1", result);
        assertEquals("1", jedis.get("a"));
    }

}