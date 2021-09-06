package org.obapanel.jedis.utils.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.utils.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.utils.functional.JedisTestFactory.createJedisPool;
import static org.obapanel.jedis.utils.functional.JedisTestFactory.functionalTestEnabled;


public class FunctionalScanUtilTest {



    private static final Logger LOG = LoggerFactory.getLogger(FunctionalScanUtilTest.class);

    private JedisPool jedisPool;
    private String varName;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(functionalTestEnabled());
        if (!functionalTestEnabled()) return;
        jedisPool = createJedisPool();
        varName = "scan:" + this.getClass().getName() + ":" + System.currentTimeMillis() + "_";
    }

    @After
    public void after() throws IOException {
        if (!functionalTestEnabled()) return;
        if (jedisPool != null) {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.del(varName + "a");
                jedis.del(varName + "b");
                jedis.del(varName + "c");
            }
            jedisPool.close();
        }
    }

    @Test
    public void retrieveListOfKeys1Test() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(varName + "a","1");
            jedis.set(varName + "b","2");
            jedis.set(varName + "c","3");
        }
        List<String> keys;
        try (Jedis jedis = jedisPool.getResource()) {
            keys = ScanUtil.retrieveListOfKeys(jedis, varName + "*");
        }
        assertEquals(3, keys.size());
        assertTrue(keys.contains(varName + "a"));
        assertTrue(keys.contains(varName + "b"));
        assertTrue(keys.contains(varName + "c"));
    }


    @Test
    public void retrieveListOfKeys2Test() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(varName + "a","1");
            jedis.set(varName + "b","2");
            jedis.set(varName + "c","3");
        }
        List<String> keys = ScanUtil.retrieveListOfKeys(jedisPool, varName + "*");
        assertEquals(3, keys.size());
        assertTrue(keys.contains(varName + "a"));
        assertTrue(keys.contains(varName + "b"));
        assertTrue(keys.contains(varName + "c"));
    }

    @Test
    public void retrieveListOfKeys3Test() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(varName + "a","1");
            jedis.set(varName + "b","2");
            jedis.set(varName + "c","3");
        }
        List<String> keys = ScanUtil.retrieveListOfKeys(jedisPool, varName + "a*");
        assertEquals(1, keys.size());
        assertTrue(keys.contains(varName + "a"));
    }

    @Test
    public void useListOfKeys1Test() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(varName + "a","1");
            jedis.set(varName + "b","2");
            jedis.set(varName + "c","3");
        }
        List<String> keys = new ArrayList<>();
        try (Jedis jedis = jedisPool.getResource()) {
            ScanUtil.useListOfKeys(jedis, varName + "*", k -> {
                if ( k != null && !k.isEmpty()) {
                    keys.add(k);
                }
            });
        }
        assertEquals(3, keys.size());
        assertTrue(keys.contains(varName + "a"));
        assertTrue(keys.contains(varName + "b"));
        assertTrue(keys.contains(varName + "c"));
    }

    @Test
    public void useListOfKeys2Test() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(varName + "a","1");
            jedis.set(varName + "b","2");
            jedis.set(varName + "c","3");
        }
        List<String> keys = new ArrayList<>();
        ScanUtil.useListOfKeys(jedisPool, varName + "*", k -> {
            if ( k != null && !k.isEmpty()) {
                keys.add(k);
            }
        });
        assertEquals(3, keys.size());
        assertTrue(keys.contains(varName + "a"));
        assertTrue(keys.contains(varName + "b"));
        assertTrue(keys.contains(varName + "c"));
    }

    @Test
    public void useListOfKeys3Test() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(varName + "a","1");
            jedis.set(varName + "b","2");
            jedis.set(varName + "c","3");
        }
        List<String> keys = new ArrayList<>();
        ScanUtil.useListOfKeys(jedisPool, varName + "c*", k -> {
            if ( k != null && !k.isEmpty()) {
                keys.add(k);
            }
        });
        assertEquals(1, keys.size());
        assertTrue(keys.contains(varName + "c"));
    }


}
