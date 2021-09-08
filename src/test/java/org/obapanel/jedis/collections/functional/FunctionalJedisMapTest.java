package org.obapanel.jedis.collections.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.collections.JedisMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.collections.functional.JedisTestFactory.createJedisPool;
import static org.obapanel.jedis.collections.functional.JedisTestFactory.functionalTestEnabled;

public class FunctionalJedisMapTest {

    private static final Logger LOG = LoggerFactory.getLogger(FunctionalJedisMapTest.class);

    private String mapName;
    private JedisPool jedisPool;

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(functionalTestEnabled());
        if (!functionalTestEnabled()) return;
        mapName = "map:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        jedisPool = createJedisPool();
    }

    @After
    public void after() {
        if (jedisPool != null) {
            try(Jedis jedis = jedisPool.getResource()) {
                jedis.del(mapName);
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
