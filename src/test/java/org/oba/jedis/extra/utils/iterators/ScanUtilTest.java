package org.oba.jedis.extra.utils.iterators;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ScanUtilTest {



    private static final Logger LOGGER = LoggerFactory.getLogger(ScanUtilTest.class);

    private MockOfJedis mockOfJedis;
    private JedisPooled jedisPooled;


    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(MockOfJedis.unitTestEnabled());
        if (!MockOfJedis.unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
        jedisPooled = mockOfJedis.getJedisPooled();
    }

    @After
    public void after() throws IOException {
        if (jedisPooled != null) {
            jedisPooled.close();
        }
        if (mockOfJedis != null) {
            mockOfJedis.clearData();
        }
    }

    @Test
    public void retrieveListOfKeys1Test() {
        mockOfJedis.getJedisPooled().set("a","1");
        mockOfJedis.getJedisPooled().set("b","2");
        mockOfJedis.getJedisPooled().set("c","3");
        List<String> keys;
        keys = ScanUtil.retrieveListOfKeys(mockOfJedis.getJedisPooled(), "*");
        assertEquals(3, keys.size());
        assertTrue(keys.contains("a"));
        assertTrue(keys.contains("b"));
        assertTrue(keys.contains("c"));
    }


    @Test
    public void retrieveListOfKeys2Test() {
        mockOfJedis.getJedisPooled().set("a","1");
        mockOfJedis.getJedisPooled().set("b","2");
        mockOfJedis.getJedisPooled().set("c","3");
        List<String> keys = ScanUtil.retrieveListOfKeys(jedisPooled, "*");
        assertEquals(3, keys.size());
        assertTrue(keys.contains("a"));
        assertTrue(keys.contains("b"));
        assertTrue(keys.contains("c"));
    }

    @Test
    public void retrieveListOfKeys3Test() {
        mockOfJedis.getJedisPooled().set("a","1");
        mockOfJedis.getJedisPooled().set("b","2");
        mockOfJedis.getJedisPooled().set("c","3");
        List<String> keys = ScanUtil.retrieveListOfKeys(jedisPooled, "a*");
        assertEquals(1, keys.size());
        assertTrue(keys.contains("a"));
    }

    @Test
    public void useListOfKeys1Test() {
        mockOfJedis.getJedisPooled().set("a","1");
        mockOfJedis.getJedisPooled().set("b","2");
        mockOfJedis.getJedisPooled().set("c","3");
        List<String> keys = new ArrayList<>();
        ScanUtil.useListOfKeys(mockOfJedis.getJedisPooled(), "*", k -> {
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
    public void useListOfKeys2Test() {
        mockOfJedis.getJedisPooled().set("a","1");
        mockOfJedis.getJedisPooled().set("b","2");
        mockOfJedis.getJedisPooled().set("c","3");
        List<String> keys = new ArrayList<>();
        ScanUtil.useListOfKeys(jedisPooled, "*", k -> {
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
        mockOfJedis.getJedisPooled().set("a","1");
        mockOfJedis.getJedisPooled().set("b","2");
        mockOfJedis.getJedisPooled().set("c","3");
        List<String> keys = new ArrayList<>();
        ScanUtil.useListOfKeys(jedisPooled, "c*", k -> {
            if ( k != null && !k.isEmpty()) {
                keys.add(k);
            }
        });
        assertEquals(1, keys.size());
        assertTrue(keys.contains("c"));
    }


}
