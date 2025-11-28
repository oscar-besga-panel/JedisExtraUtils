package org.oba.jedis.extra.utils.iterators.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.iterators.ScanUtil;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.oba.jedis.extra.utils.test.WithJedisPoolDelete;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPooled;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class FunctionalScanUtilTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalScanUtilTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private JedisPooled jedisPooled;
    private String varName;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPooled = jtfTest.createJedisPooled();
        varName = "scan:" + this.getClass().getName() + ":" + System.currentTimeMillis() + "_";
    }

    @After
    public void after() throws IOException {
        if (!jtfTest.functionalTestEnabled()) return;
        if (jedisPooled != null) {
            jedisPooled.del(varName + "a");
            jedisPooled.del(varName + "b");
            jedisPooled.del(varName + "c");
            jedisPooled.close();
        }
    }

    @Test
    public void retrieveListOfKeys1Test() {
        jedisPooled.set(varName + "a","1");
        jedisPooled.set(varName + "b","2");
        jedisPooled.set(varName + "c","3");
        List<String> keys;
        keys = ScanUtil.retrieveListOfKeys(jedisPooled, varName + "*");
        assertEquals(3, keys.size());
        assertTrue(keys.contains(varName + "a"));
        assertTrue(keys.contains(varName + "b"));
        assertTrue(keys.contains(varName + "c"));
    }


    @Test
    public void retrieveListOfKeys2Test() {
        jedisPooled.set(varName + "a","1");
        jedisPooled.set(varName + "b","2");
        jedisPooled.set(varName + "c","3");
        List<String> keys = ScanUtil.retrieveListOfKeys(jedisPooled, varName + "*");
        assertEquals(3, keys.size());
        assertTrue(keys.contains(varName + "a"));
        assertTrue(keys.contains(varName + "b"));
        assertTrue(keys.contains(varName + "c"));
    }

    @Test
    public void retrieveListOfKeys3Test() {
        jedisPooled.set(varName + "a","1");
        jedisPooled.set(varName + "b","2");
        jedisPooled.set(varName + "c","3");
        List<String> keys = ScanUtil.retrieveListOfKeys(jedisPooled, varName + "a*");
        assertEquals(1, keys.size());
        assertTrue(keys.contains(varName + "a"));
    }

    @Test
    public void useListOfKeys1Test() {
        jedisPooled.set(varName + "a","1");
        jedisPooled.set(varName + "b","2");
        jedisPooled.set(varName + "c","3");
        List<String> keys = new ArrayList<>();
        ScanUtil.useListOfKeys(jedisPooled, varName + "*", k -> {
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
        jedisPooled.set(varName + "a","1");
        jedisPooled.set(varName + "b","2");
        jedisPooled.set(varName + "c","3");
        List<String> keys = new ArrayList<>();
        ScanUtil.useListOfKeys(jedisPooled, varName + "*", k -> {
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
        jedisPooled.set(varName + "a","1");
        jedisPooled.set(varName + "b","2");
        jedisPooled.set(varName + "c","3");
        List<String> keys = new ArrayList<>();
        ScanUtil.useListOfKeys(jedisPooled, varName + "c*", k -> {
            if ( k != null && !k.isEmpty()) {
                keys.add(k);
            }
        });
        assertEquals(1, keys.size());
        assertTrue(keys.contains(varName + "c"));
    }


}
