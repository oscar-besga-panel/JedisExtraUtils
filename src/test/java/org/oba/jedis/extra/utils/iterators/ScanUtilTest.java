package org.oba.jedis.extra.utils.iterators;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.UnifiedJedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ScanUtilTest {



    private static final Logger LOGGER = LoggerFactory.getLogger(ScanUtilTest.class);

    private MockOfJedis mockOfJedis;
    private UnifiedJedis redisClient;


    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(MockOfJedis.unitTestEnabled());
        if (!MockOfJedis.unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
        redisClient = mockOfJedis.getRedisClient();
    }

    @After
    public void after() throws IOException {
        if (redisClient != null) {
            redisClient.close();
        }
        if (mockOfJedis != null) {
            mockOfJedis.clearData();
        }
    }

    @Test
    public void retrieveListOfKeys1Test() {
        mockOfJedis.getRedisClient().set("a","1");
        mockOfJedis.getRedisClient().set("b","2");
        mockOfJedis.getRedisClient().set("c","3");
        List<String> keys;
        keys = ScanUtil.retrieveListOfKeys(mockOfJedis.getRedisClient(), "*");
        assertEquals(3, keys.size());
        assertTrue(keys.contains("a"));
        assertTrue(keys.contains("b"));
        assertTrue(keys.contains("c"));
    }


    @Test
    public void retrieveListOfKeys2Test() {
        mockOfJedis.getRedisClient().set("a","1");
        mockOfJedis.getRedisClient().set("b","2");
        mockOfJedis.getRedisClient().set("c","3");
        List<String> keys = ScanUtil.retrieveListOfKeys(redisClient, "*");
        assertEquals(3, keys.size());
        assertTrue(keys.contains("a"));
        assertTrue(keys.contains("b"));
        assertTrue(keys.contains("c"));
    }

    @Test
    public void retrieveListOfKeys3Test() {
        mockOfJedis.getRedisClient().set("a","1");
        mockOfJedis.getRedisClient().set("b","2");
        mockOfJedis.getRedisClient().set("c","3");
        List<String> keys = ScanUtil.retrieveListOfKeys(redisClient, "a*");
        assertEquals(1, keys.size());
        assertTrue(keys.contains("a"));
    }

    @Test
    public void useListOfKeys1Test() {
        mockOfJedis.getRedisClient().set("a","1");
        mockOfJedis.getRedisClient().set("b","2");
        mockOfJedis.getRedisClient().set("c","3");
        List<String> keys = new ArrayList<>();
        ScanUtil.useListOfKeys(mockOfJedis.getRedisClient(), "*", k -> {
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
        mockOfJedis.getRedisClient().set("a","1");
        mockOfJedis.getRedisClient().set("b","2");
        mockOfJedis.getRedisClient().set("c","3");
        List<String> keys = new ArrayList<>();
        ScanUtil.useListOfKeys(redisClient, "*", k -> {
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
        mockOfJedis.getRedisClient().set("a","1");
        mockOfJedis.getRedisClient().set("b","2");
        mockOfJedis.getRedisClient().set("c","3");
        List<String> keys = new ArrayList<>();
        ScanUtil.useListOfKeys(redisClient, "c*", k -> {
            if ( k != null && !k.isEmpty()) {
                keys.add(k);
            }
        });
        assertEquals(1, keys.size());
        assertTrue(keys.contains("c"));
    }


}
