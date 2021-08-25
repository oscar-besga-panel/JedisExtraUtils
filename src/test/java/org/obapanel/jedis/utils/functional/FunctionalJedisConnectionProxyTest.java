package org.obapanel.jedis.utils.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.utils.JedisPoolAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.utils.functional.JedisTestFactory.createJedisClient;
import static org.obapanel.jedis.utils.functional.JedisTestFactory.createJedisPool;
import static org.obapanel.jedis.utils.functional.JedisTestFactory.functionalTestEnabled;

public class FunctionalJedisConnectionProxyTest {

    private static final Logger LOG = LoggerFactory.getLogger(FunctionalJedisConnectionProxyTest.class);


    private JedisPool jedisPool;
    private Jedis jedis;
    private String varName;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(functionalTestEnabled());
        if (!functionalTestEnabled()) return;
        jedisPool = createJedisPool();
        jedis = createJedisClient();
        varName = "scan:" + this.getClass().getName() + ":" + System.currentTimeMillis() + "_";
    }

    @After
    public void after() throws IOException {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(varName + "a");
            jedis.del(varName + "b");
            jedis.del(varName + "c");
        }
        jedisPool.close();
        jedis.close();
    }


    @Test
    public void testProxy1() {
        JedisPool jedisPoolButReallyIsAProxy = JedisPoolAdapter.poolFromJedis(jedis);
        try(Jedis jedisProxyConnection = jedisPoolButReallyIsAProxy.getResource()){
            jedisProxyConnection.set(varName + "a","1");
            jedisProxyConnection.set(varName + "b","2");
            jedisProxyConnection.set(varName + "c","3");
        }
        assertTrue(jedis.exists(varName + "a"));
        assertTrue(jedis.exists(varName + "b"));
        assertTrue(jedis.exists(varName + "c"));
    }

    @Test
    public void testProxy2() {
        JedisPoolAdapter.poolFromJedis(jedis).withResource( jedisProxyConnection ->{
            jedisProxyConnection.set(varName + "a","1");
            jedisProxyConnection.set(varName + "b","2");
            jedisProxyConnection.set(varName + "c","3");
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
                withResourceFunction( jedisProxyConnection -> {
                    return jedisProxyConnection.get(varName + "a");
                });
        assertEquals("1", result);
        assertEquals("1", jedis.get(varName + "a"));
    }

}