package org.obapanel.jedis.mapper.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.mapper.MapSaver;
import org.obapanel.jedis.mapper.PojoDataTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.mapper.functional.JedisTestFactory.createJedisClient;
import static org.obapanel.jedis.mapper.functional.JedisTestFactory.createJedisPool;
import static org.obapanel.jedis.mapper.functional.JedisTestFactory.functionalTestEnabled;


public class FunctionalJedisMapperTest {


    private static final Logger LOG = LoggerFactory.getLogger(FunctionalJedisMapperTest.class);

    private String keyName;
    private Jedis jedis;
    private JedisPool jedisPool;

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(functionalTestEnabled());
        if (!functionalTestEnabled()) return;
        keyName = "pojoDataTest:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        jedis = createJedisClient();
        jedisPool = createJedisPool();
    }

    @After
    public void after() {
        if (jedis != null) {
            //jedis.del(keyName);
            jedis.close();
        }
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    @Test
    public void testLoad() {
        PojoDataTest newPojoDataTest = PojoDataTest.pojoDataTestExample1;
        MapSaver mapSaver = new MapSaver(jedis);
        mapSaver.save(keyName, newPojoDataTest);
        PojoDataTest jedisPojoDataTest = mapSaver.load(keyName, PojoDataTest.class);
        assertNotNull(jedisPojoDataTest);
        assertTrue(jedis.exists(keyName));
        assertEquals(newPojoDataTest.getName(), jedisPojoDataTest.getName());
        assertEquals(newPojoDataTest.getIntNum(), jedisPojoDataTest.getIntNum());
        assertEquals(newPojoDataTest.getWeigth(), jedisPojoDataTest.getWeigth());
        assertTrue(newPojoDataTest.getDoubleNum() == jedisPojoDataTest.getDoubleNum());
        assertEquals(newPojoDataTest.getWeigth(), jedisPojoDataTest.getWeigth());
        assertTrue(newPojoDataTest.equals(jedisPojoDataTest));
    }

    @Test
    public void testLoad2() {
        PojoDataTest pojoDataTest = new PojoDataTest(PojoDataTest.pojoDataTestExample2);
        pojoDataTest.setName(null);
        MapSaver mapSaver = new MapSaver(jedis);
        mapSaver.save(keyName, pojoDataTest);
        PojoDataTest jedisPojoDataTest = mapSaver.load(keyName, PojoDataTest.class);
        assertEquals(pojoDataTest, jedisPojoDataTest);
    }

    @Test
    public void testRetrieveLoad() {
        PojoDataTest newPojoDataTest = PojoDataTest.pojoDataTestExample1;
        MapSaver mapSaver = new MapSaver(jedis);
        mapSaver.save(keyName, newPojoDataTest);
        PojoDataTest jedisPojoDataTest11 = mapSaver.load(keyName, PojoDataTest.class);
        assertNotNull(jedisPojoDataTest11);
        assertTrue(jedis.exists(keyName));
        assertTrue(newPojoDataTest.equals(jedisPojoDataTest11));
        PojoDataTest jedisPojoDataTest12 = mapSaver.retrieve(keyName, PojoDataTest.class);
        assertNotNull(jedisPojoDataTest12);
        assertFalse(jedis.exists(keyName));
        assertTrue(newPojoDataTest.equals(jedisPojoDataTest12));
    }

}
