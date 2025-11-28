package org.oba.jedis.extra.utils.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.oba.jedis.extra.utils.utils.MockOfJedis.unitTestEnabled;

public class JedisSentinelPoolAdapterTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisSentinelPoolAdapterTest.class);

    private MockOfJedis mockOfJedis;
    private JedisPool jedisPool;
    private JedisSentinelPool jedisSentinelPool;
    private Jedis jedis;


    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(unitTestEnabled());
        if (!unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
        jedisPool = mockOfJedis.getJedisPooled();
        jedisSentinelPool = mockOfJedis.getJedisSentinelPool();
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
        JedisPool jedisPoolButReallyIsAProxy = JedisSentinelPoolAdapter.poolFromSentinel(jedisSentinelPool);
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
        JedisSentinelPoolAdapter.poolFromSentinel(jedisSentinelPool).withResource( jedisProxyConnection ->{
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
        String result = JedisSentinelPoolAdapter.poolFromSentinel(jedisSentinelPool).
                withResourceFunction( jedisProxyConnection -> {
            return jedisProxyConnection.get("a");
        });
        assertEquals("1", result);
        assertEquals("1", jedis.get("a"));
    }

}