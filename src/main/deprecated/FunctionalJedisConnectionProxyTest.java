package org.oba.jedis.extra.utils.utils.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.oba.jedis.extra.utils.test.WithJedisPoolDelete;
import org.oba.jedis.extra.utils.utils.JedisPoolAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FunctionalJedisConnectionProxyTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalJedisConnectionProxyTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private JedisPool jedisPool;
    private Jedis jedis;
    private String varName;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPool = jtfTest.createJedisPool();
        jedis = jtfTest.createJedisClient();
        varName = "scan:" + this.getClass().getName() + ":" + System.currentTimeMillis() + "_";
    }

    @After
    public void after() throws IOException {
        if (!jtfTest.functionalTestEnabled()) return;
        if (jedisPool != null) {
            WithJedisPoolDelete.doDelete(jedisPool, varName + "a");
            WithJedisPoolDelete.doDelete(jedisPool, varName + "b");
            WithJedisPoolDelete.doDelete(jedisPool, varName + "c");
            jedisPool.close();
        }
        if (jedis != null) {
            jedis.close();
        }
    }


    @Test
    public void testProxy1() {
        JedisPool jedisPoolButReallyIsAProxy = JedisPoolAdapter.poolFromJedis(jedis);
        try (Jedis jedisProxyConnection = jedisPoolButReallyIsAProxy.getResource()) {
            jedisProxyConnection.set(varName + "a", "1");
            jedisProxyConnection.set(varName + "b", "2");
            jedisProxyConnection.set(varName + "c", "3");
        }
        assertTrue(jedis.exists(varName + "a"));
        assertTrue(jedis.exists(varName + "b"));
        assertTrue(jedis.exists(varName + "c"));
    }

    @Test
    public void testProxy2() {
        JedisPoolAdapter.poolFromJedis(jedis).withResource(jedisProxyConnection -> {
            jedisProxyConnection.set(varName + "a", "1");
            jedisProxyConnection.set(varName + "b", "2");
            jedisProxyConnection.set(varName + "c", "3");
        });
        assertTrue(jedis.exists(varName + "a"));
        assertTrue(jedis.exists(varName + "b"));
        assertTrue(jedis.exists(varName + "c"));
    }

    @Test
    public void testProxy3() {
        jedis.set(varName + "a", "1");
        String result = JedisPoolAdapter.
                poolFromJedis(jedis).
                withResourceFunction(jedisProxyConnection -> {
                    return jedisProxyConnection.get(varName + "a");
                });
        assertEquals("1", result);
        assertEquals("1", jedis.get(varName + "a"));
    }

    @Test(expected = IllegalStateException.class)
    public void testProxy4() {
        JedisPool jedisPool = JedisPoolAdapter.poolFromJedis(jedis);
        jedisPool.close();
        jedisPool.getResource();
    }

}