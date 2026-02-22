package org.oba.jedis.extra.utils.iterators.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.iterators.ScanUtil;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.RedisClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class FunctionalScanUtilTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalScanUtilTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private RedisClient redisClient;
    private String varName;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        redisClient = jtfTest.createRedisClient();
        varName = "scan:" + this.getClass().getName() + ":" + System.currentTimeMillis() + "_";
    }

    @After
    public void after() throws IOException {
        if (!jtfTest.functionalTestEnabled()) return;
        if (redisClient != null) {
            redisClient.del(varName + "a");
            redisClient.del(varName + "b");
            redisClient.del(varName + "c");
            redisClient.close();
        }
    }

    @Test
    public void retrieveListOfKeys1Test() {
        redisClient.set(varName + "a","1");
        redisClient.set(varName + "b","2");
        redisClient.set(varName + "c","3");
        List<String> keys;
        keys = ScanUtil.retrieveListOfKeys(redisClient, varName + "*");
        assertEquals(3, keys.size());
        assertTrue(keys.contains(varName + "a"));
        assertTrue(keys.contains(varName + "b"));
        assertTrue(keys.contains(varName + "c"));
    }


    @Test
    public void retrieveListOfKeys2Test() {
        redisClient.set(varName + "a","1");
        redisClient.set(varName + "b","2");
        redisClient.set(varName + "c","3");
        List<String> keys = ScanUtil.retrieveListOfKeys(redisClient, varName + "*");
        assertEquals(3, keys.size());
        assertTrue(keys.contains(varName + "a"));
        assertTrue(keys.contains(varName + "b"));
        assertTrue(keys.contains(varName + "c"));
    }

    @Test
    public void retrieveListOfKeys3Test() {
        redisClient.set(varName + "a","1");
        redisClient.set(varName + "b","2");
        redisClient.set(varName + "c","3");
        List<String> keys = ScanUtil.retrieveListOfKeys(redisClient, varName + "a*");
        assertEquals(1, keys.size());
        assertTrue(keys.contains(varName + "a"));
    }

    @Test
    public void useListOfKeys1Test() {
        redisClient.set(varName + "a","1");
        redisClient.set(varName + "b","2");
        redisClient.set(varName + "c","3");
        List<String> keys = new ArrayList<>();
        ScanUtil.useListOfKeys(redisClient, varName + "*", k -> {
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
    public void useListOfKeys2Test() {
        redisClient.set(varName + "a","1");
        redisClient.set(varName + "b","2");
        redisClient.set(varName + "c","3");
        List<String> keys = new ArrayList<>();
        ScanUtil.useListOfKeys(redisClient, varName + "*", k -> {
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
        redisClient.set(varName + "a","1");
        redisClient.set(varName + "b","2");
        redisClient.set(varName + "c","3");
        List<String> keys = new ArrayList<>();
        ScanUtil.useListOfKeys(redisClient, varName + "c*", k -> {
            if ( k != null && !k.isEmpty()) {
                keys.add(k);
            }
        });
        assertEquals(1, keys.size());
        assertTrue(keys.contains(varName + "c"));
    }


}
