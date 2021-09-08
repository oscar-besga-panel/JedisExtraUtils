package org.obapanel.jedis.collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.collections.MockOfJedisForList.unitTestEnabledForList;

public class JedisMapTest {

    private String mapName, mapName2;
    private MockOfJedisForMap mockOfJedisForMap;

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(unitTestEnabledForList());
        if (!unitTestEnabledForList()) return;
        mapName = "map:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        mapName2 = "map2:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        mockOfJedisForMap = new MockOfJedisForMap();
    }

    @After
    public void after() {
        if (mockOfJedisForMap != null) mockOfJedisForMap.clearData();
    }



    JedisMap createABCMap() {
        JedisMap jedisMap = new JedisMap(mockOfJedisForMap.getJedisPool(), mapName);
        jedisMap.put("a","1");
        jedisMap.put("b","2");
        jedisMap.put("c","3");
        return jedisMap;
    }


    @Test(expected = IllegalStateException.class)
    public void basicTestWithErrorExists() {
        JedisMap jedisMap = new JedisMap(mockOfJedisForMap.getJedisPool(), mapName);
        jedisMap.checkExists();
    }


    @Test
    public void basicTestExists() {
        JedisMap jedisMap = createABCMap();
        assertTrue(jedisMap.exists());
    }

    @Test
    public void basicTestGet() {
        JedisMap jedisMap = createABCMap();
        assertEquals("1", jedisMap.get("a"));
        assertNull(jedisMap.get("d"));
    }

    @Test
    public void basicTestSize() {
        JedisMap jedisMap = createABCMap();
        int size1 = jedisMap.size();
        jedisMap.put("d","4");
        int size2 = jedisMap.size();
        JedisMap jedisMap2 = new JedisMap(mockOfJedisForMap.getJedisPool(), mapName2);
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

}
