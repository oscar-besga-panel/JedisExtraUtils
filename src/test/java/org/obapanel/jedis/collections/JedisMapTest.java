package org.obapanel.jedis.collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.collections.MockOfJedisForList.unitTestEnabledForList;

public class JedisMapTest {

    private String mapName;
    private MockOfJedisForMap mockOfJedisForMap;

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(unitTestEnabledForList());
        if (!unitTestEnabledForList()) return;
        mapName = "list:" + this.getClass().getName() + ":" + System.currentTimeMillis();
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

}
