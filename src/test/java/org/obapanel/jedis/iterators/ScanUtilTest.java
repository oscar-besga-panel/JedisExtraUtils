package org.obapanel.jedis.iterators;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.utils.MockOfJedis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.iterators.MockOfJedis.unitTestEnabled;


public class ScanUtilTest {



    private static final Logger LOGGER = LoggerFactory.getLogger(ScanUtilTest.class);

    private org.obapanel.jedis.utils.MockOfJedis mockOfJedis;
    private JedisPool jedisPool;


    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(unitTestEnabled());
        if (!unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
        jedisPool = mockOfJedis.getJedisPool();
    }

    @After
    public void after() throws IOException {
        if (jedisPool != null) {
            jedisPool.close();
        }
        if (mockOfJedis != null) {
            mockOfJedis.clearData();
        }
    }

    @Test
    public void retrieveListOfKeys1Test() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set("a","1");
            jedis.set("b","2");
            jedis.set("c","3");
        }
        List<String> keys;
        try (Jedis jedis = jedisPool.getResource()) {
            keys = ScanUtil.retrieveListOfKeys(jedis, "*");
        }
        assertEquals(3, keys.size());
        assertTrue(keys.contains("a"));
        assertTrue(keys.contains("b"));
        assertTrue(keys.contains("c"));
    }


    @Test
    public void retrieveListOfKeys2Test() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set("a","1");
            jedis.set("b","2");
            jedis.set("c","3");
        }
        List<String> keys = ScanUtil.retrieveListOfKeys(jedisPool, "*");
        assertEquals(3, keys.size());
        assertTrue(keys.contains("a"));
        assertTrue(keys.contains("b"));
        assertTrue(keys.contains("c"));
    }

    @Test
    public void retrieveListOfKeys3Test() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set("a","1");
            jedis.set("b","2");
            jedis.set("c","3");
        }
        // Hey ! here, because we use Java mocks, we use Java regexp !!
        List<String> keys = ScanUtil.retrieveListOfKeys(jedisPool, "a.*");
        assertEquals(1, keys.size());
        assertTrue(keys.contains("a"));
    }

    @Test
    public void useListOfKeys1Test() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set("a","1");
            jedis.set("b","2");
            jedis.set("c","3");
        }
        List<String> keys = new ArrayList<>();
        try (Jedis jedis = jedisPool.getResource()) {
            ScanUtil.useListOfKeys(jedis, "*", k -> {
                if ( k != null && !k.isEmpty()) {
                    keys.add(k);
                }
            });
        }
        assertEquals(3, keys.size());
        assertTrue(keys.contains("a"));
        assertTrue(keys.contains("b"));
        assertTrue(keys.contains("c"));
    }

    @Test
    public void useListOfKeys2Test() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set("a","1");
            jedis.set("b","2");
            jedis.set("c","3");
        }
        List<String> keys = new ArrayList<>();
        ScanUtil.useListOfKeys(jedisPool, "*", k -> {
            if ( k != null && !k.isEmpty()) {
                keys.add(k);
            }
        });
        assertEquals(3, keys.size());
        assertTrue(keys.contains("a"));
        assertTrue(keys.contains("b"));
        assertTrue(keys.contains("c"));
    }

    @Test
    public void useListOfKeys3Test() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set("a","1");
            jedis.set("b","2");
            jedis.set("c","3");
        }
        List<String> keys = new ArrayList<>();
        ScanUtil.useListOfKeys(jedisPool, "c.*", k -> {
            if ( k != null && !k.isEmpty()) {
                keys.add(k);
            }
        });
        assertEquals(1, keys.size());
        assertTrue(keys.contains("c"));
    }


}
