package org.oba.jedis.extra.utils.collections.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.collections.JedisMap;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FunctionalJedisMapTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalJedisMapTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private String mapName, mapName2;
    private JedisPool jedisPool;

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        mapName = "map:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        mapName2 = "map2:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        jedisPool = jtfTest.createJedisPool();
    }

    @After
    public void after() {
        if (jedisPool != null) {
            try(Jedis jedis = jedisPool.getResource()) {
                jedis.del(mapName, mapName2);
            }
            jedisPool.close();
        }
    }


    JedisMap createABCMap() {
        JedisMap jedisMap = new JedisMap(jedisPool, mapName);
        jedisMap.put("a","1");
        jedisMap.put("b","2");
        jedisMap.put("c","3");
        return jedisMap;
    }

    @Test
    public void basicForEachMap(){
        StringBuilder sb = new StringBuilder();
        JedisMap jedisMap = createABCMap();
        jedisMap.forEach( (k, v) -> sb.append(k).append(v));
        assertEquals("a1b2c3", sb.toString());
    }

    @Test(expected = IllegalStateException.class)
    public void basicTestWithErrorExists() {
        JedisMap jedisMap = new JedisMap(jedisPool, mapName);
        jedisMap.checkExists();
    }


    @Test
    public void basicTestExists() {
        JedisMap jedisMap = createABCMap();
        assertTrue(jedisMap.exists());
    }

    @Test
    public void basicTestContainsKey() {
        JedisMap jedisMap = createABCMap();
        assertTrue(jedisMap.containsKey("a"));
        assertFalse(jedisMap.containsKey("d"));
    }


    @Test
    public void basicTestSize() {
        JedisMap jedisMap = createABCMap();
        int size1 = jedisMap.size();
        jedisMap.put("d","4");
        int size2 = jedisMap.size();
        JedisMap jedisMap2 = new JedisMap(jedisPool, mapName2);
        assertEquals(3, size1);
        assertEquals(4, size2);
        assertFalse(jedisMap.isEmpty());
        jedisMap.clear();
        assertTrue(jedisMap.isEmpty());
        assertEquals(0, jedisMap.size());
        assertTrue(jedisMap2.isEmpty());
        assertEquals(0, jedisMap2.size());
    }

    @Test
    public void basicTestGet() {
        JedisMap jedisMap = createABCMap();
        assertEquals("1", jedisMap.get("a"));
        assertNull(jedisMap.get("d"));
    }

    @Test
    public void basicTestPut() {
        JedisMap jedisMap = createABCMap();
        String previous1 = jedisMap.put("d","4");
        assertEquals("1", jedisMap.get("a"));
        assertEquals("4", jedisMap.get("d"));
        assertNull(jedisMap.get("e"));
        assertNull(previous1);
        String previous2 = jedisMap.put("a","2");
        assertEquals("2", jedisMap.get("a"));
        assertEquals("1", previous2);
        assertNull(jedisMap.get("e"));
    }

    @Test
    public void basicTestDel() {
        JedisMap jedisMap = createABCMap();
        String data1 = jedisMap.remove("a");
        String data2 = jedisMap.remove("x");
        String data3 = jedisMap.remove("a");
        assertEquals("1", data1);
        assertNull(data2);
        assertNull(data3);
        assertNull(jedisMap.get("a"));
        assertNotNull(jedisMap.get("b"));
    }

    @Test
    public void testPutAll() {
        Map<String, String> extra = new HashMap<>();
        extra.put("x","97");
        extra.put("y","98");
        extra.put("z","99");
        JedisMap jedisMap = createABCMap();
        jedisMap.putAll(extra);
        assertEquals("1", jedisMap.get("a"));
        assertEquals("2", jedisMap.get("b"));
        assertEquals("99", jedisMap.get("z"));
        assertEquals("97", jedisMap.get("x"));
    }

    @Test
    public void testEntrySet() {
        JedisMap jedisMap = createABCMap();
        Set<Map.Entry<String, String>> entries = jedisMap.entrySet();
        assertEquals(3, entries.size());
        assertEquals(jedisMap.size(), entries.size());
        assertTrue(entries.contains(new AbstractMap.SimpleImmutableEntry<>("a","1")));
        assertTrue(entries.contains(new AbstractMap.SimpleImmutableEntry<>("b","2")));
        assertTrue(entries.contains(new AbstractMap.SimpleImmutableEntry<>("c","3")));
        jedisMap.clear();
        entries = jedisMap.entrySet();
        assertTrue(entries.isEmpty());
    }

    @Test
    public void testKeySet() {
        JedisMap jedisMap = createABCMap();
        Set<String> keys = jedisMap.keySet();
        assertEquals(3, keys.size());
        assertEquals(jedisMap.size(), keys.size());
        assertTrue(keys.contains("a"));
        assertTrue(keys.contains("b"));
        assertTrue(keys.contains("c"));
        jedisMap.clear();
        keys = jedisMap.keySet();
        assertTrue(keys.isEmpty());
    }

    @Test
    public void testValues() {
        JedisMap jedisMap = createABCMap();
        Collection<String> values = jedisMap.values();
        assertEquals(3, values.size());
        assertEquals(jedisMap.size(), values.size());
        assertTrue(values.contains("1"));
        assertTrue(values.contains("2"));
        assertTrue(values.contains("3"));
        jedisMap.clear();
        values = jedisMap.values();
        assertTrue(values.isEmpty());
    }

}
