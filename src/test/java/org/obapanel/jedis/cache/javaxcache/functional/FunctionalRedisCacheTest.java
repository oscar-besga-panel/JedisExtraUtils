package org.obapanel.jedis.cache.javaxcache.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.obapanel.jedis.cache.javaxcache.MockOfJedisForRedisCache;
import org.obapanel.jedis.cache.javaxcache.RedisCache;
import org.obapanel.jedis.cache.javaxcache.RedisCacheConfiguration;
import org.obapanel.jedis.cache.javaxcache.RedisCacheManager;
import org.obapanel.jedis.cache.javaxcache.RedisCachingProvider;
import org.obapanel.jedis.collections.functional.FunctionalJedisListIteratorTest;
import org.obapanel.jedis.common.test.JedisTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.cache.processor.EntryProcessorResult;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FunctionalRedisCacheTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalRedisCacheTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();


    private JedisPool jedisPool;



    @Before
    public void before() {
        RedisCachingProvider.getInstance().getRedisCacheManager();
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPool = jtfTest.createJedisPool();
    }

    @After
    public void after() {
        if (jedisPool != null) {
            jedisPool.close();
            RedisCachingProvider.getInstance().getRedisCacheManager().clearJedisPool();
        }

    }

    private String jedisGet(String key) {
        try(Jedis jedis = jedisPool.getResource()){
            return jedis.get(key);
        }
    }


    RedisCache createNewCache() {
        String name = "cache:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        RedisCachingProvider redisCachingProvider = RedisCachingProvider.getInstance();
        RedisCacheManager redisCacheManager =  redisCachingProvider.getRedisCacheManager();
        if (!redisCacheManager.hasJedisPool()) {
            redisCacheManager.setJedisPool(jedisPool);
        }
        return redisCacheManager.createRedisCache(name , new RedisCacheConfiguration());
    }

    @Test
    public void putAndGetTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        String result = redisCache.get("a");
        assertEquals("A1", result);
        assertEquals("A1", jedisGet(redisCache.getName() + ":a"));
    }

    @Test
    public void getAllTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        redisCache.put("b","B1");
        redisCache.put("c","C1");
        Set<String> keys = new HashSet<>(Arrays.asList(new String[]{"a", "b", "c"}));
        Map<String, String> results = redisCache.getAll(keys);
        assertEquals(3, results.size());
        assertEquals("A1", results.get("a"));
        assertEquals("B1", results.get("b"));
        assertEquals("C1", results.get("c"));
        assertEquals("A1", jedisGet(redisCache.getName() + ":a"));
    }

    @Test
    public void containsKeyTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        redisCache.put("b","B1");
        assertTrue(redisCache.containsKey("a"));
        assertTrue(jedisGet(redisCache.getName() + ":a") != null);
        assertTrue(redisCache.containsKey("b"));
        assertTrue(jedisGet(redisCache.getName() + ":b") != null);
        assertFalse(redisCache.containsKey("c"));
        assertFalse(jedisGet(redisCache.getName() + ":c") != null);

    }

    @Test
    public void loadAllTest() {

    }

    @Test
    public void putTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        Set<String> keys = new HashSet<>(Arrays.asList(new String[]{"a", "b", "c"}));
        Map<String, String> results = redisCache.getAll(keys);
        assertEquals(1, results.size());
        assertEquals("A1", redisCache.get("a"));
        assertEquals("A1", results.get("a"));
        assertEquals("A1", jedisGet(redisCache.getName() + ":a"));
    }

    @Test
    public void getAndPutTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        String previous = redisCache.getAndPut("a", "A2");
        assertEquals("A1", previous);
        assertEquals("A2", redisCache.get("a"));
        assertEquals("A2", jedisGet(redisCache.getName() + ":a"));
    }

    @Test
    public void putAllTest() {
        RedisCache redisCache = createNewCache();
        Map<String, String> data = new HashMap<>();
        data.put("a","A1");
        data.put("b","B1");
        redisCache.putAll(data);
        assertEquals("A1", redisCache.get("a"));
        assertEquals("A1", jedisGet(redisCache.getName() + ":a"));
        assertEquals("B1", redisCache.get("b"));
        assertEquals("B1", jedisGet(redisCache.getName() + ":b"));
    }

    @Test
    public void putIfAbsentTest() {
        RedisCache redisCache = createNewCache();
        redisCache.putIfAbsent("a","A1");
        redisCache.putIfAbsent("a","A2");
        assertEquals("A1", redisCache.get("a"));
        assertEquals("A1", jedisGet(redisCache.getName() + ":a"));
        redisCache.put("a","A2");
        assertEquals("A2", redisCache.get("a"));
        assertEquals("A2", jedisGet(redisCache.getName() + ":a"));
    }

    @Test
    public void removeTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        assertEquals("A1", redisCache.get("a"));
        assertEquals("A1", jedisGet(redisCache.getName() + ":a"));
        redisCache.remove("a");
        assertNull( redisCache.get("a"));
        assertNull( jedisGet(redisCache.getName() + ":a"));
    }

    @Test
    public void removeTest2() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        assertEquals("A1", redisCache.get("a"));
        assertEquals("A1", jedisGet(redisCache.getName() + ":a"));
        redisCache.remove("a", "A2");
        assertEquals("A1", redisCache.get("a"));
        assertEquals("A1", jedisGet(redisCache.getName() + ":a"));
        redisCache.remove("a", "A1");
        assertNull(redisCache.get("a"));
        assertNull( jedisGet(redisCache.getName() + ":a"));
    }

    @Test
    public void getAndRemoveTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        assertEquals("A1", redisCache.get("a"));
        assertEquals("A1", jedisGet(redisCache.getName() + ":a"));
        String removed = redisCache.getAndRemove("a");
        assertNull( redisCache.get("a"));
        assertNull( jedisGet(redisCache.getName() + ":a"));
        assertEquals("A1", removed);
    }

    @Test
    public void replaceTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        assertEquals("A1", redisCache.get("a"));
        assertEquals("A1", jedisGet(redisCache.getName() + ":a"));
        boolean replaced1 = redisCache.replace("a", "A1", "A2");
        assertEquals("A2", redisCache.get("a"));
        assertEquals("A2", jedisGet(redisCache.getName() + ":a"));
        assertTrue(replaced1);
        boolean replaced2 = redisCache.replace("a", "A3", "A4");
        assertEquals("A2", redisCache.get("a"));
        assertEquals("A2", jedisGet(redisCache.getName() + ":a"));
        assertFalse(replaced2);
    }

    @Test
    public void replace2Test() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        assertEquals("A1", redisCache.get("a"));
        assertEquals("A1", jedisGet(redisCache.getName() + ":a"));
        boolean replaced1 = redisCache.replace("a", "A2");
        assertEquals("A2", redisCache.get("a"));
        assertEquals("A2", jedisGet(redisCache.getName() + ":a"));
        assertTrue(replaced1);
        boolean replaced2 = redisCache.replace("b", "B1");
        assertNull( redisCache.get("b"));
        assertNull( jedisGet(redisCache.getName() + ":b"));
        assertFalse(replaced2);
    }

    @Test
    public void getAndReplaceTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        assertEquals("A1", redisCache.get("a"));
        assertEquals("A1", jedisGet(redisCache.getName() + ":a"));
        String replaced = redisCache.getAndReplace("a","A2");
        assertEquals("A2", redisCache.get("a"));
        assertEquals("A2", jedisGet(redisCache.getName() + ":a"));
        assertEquals("A1", replaced);
    }


    @Test
    public void removeAllKeysTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        redisCache.put("b","B1");
        redisCache.put("c","C1");
        redisCache.put("d","D1");
        redisCache.put("e","E1");
        Set<String> toRemove = new HashSet<>(Arrays.asList("b", "d", "e"));
        redisCache.removeAll(toRemove);
        assertTrue(redisCache.containsKey("a"));
        assertFalse(redisCache.containsKey("b"));
        assertTrue(redisCache.containsKey("c"));
        assertFalse(redisCache.containsKey("d"));
        assertFalse(redisCache.containsKey("e"));
    }

    @Test
    public void removeAllTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        redisCache.put("b","B1");
        redisCache.put("c","C1");
        redisCache.put("d","D1");
        redisCache.put("e","E1");
        redisCache.removeAll();
        assertFalse(redisCache.containsKey("a"));
        assertFalse(redisCache.containsKey("b"));
        assertFalse(redisCache.containsKey("c"));
        assertFalse(redisCache.containsKey("d"));
        assertFalse(redisCache.containsKey("e"));
    }


    @Test
    public void clearTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        redisCache.put("b","B1");
        redisCache.put("c","C1");
        redisCache.put("d","D1");
        redisCache.put("e","E1");
        redisCache.clear();
        assertFalse(redisCache.containsKey("a"));
        assertFalse(redisCache.containsKey("b"));
        assertFalse(redisCache.containsKey("c"));
        assertFalse(redisCache.containsKey("d"));
        assertFalse(redisCache.containsKey("e"));
    }

    @Test
    public void invokeTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        String result1 = redisCache.invoke("a", (me, arg) -> me.getValue());
        String result2 = redisCache.invoke("a", (me, arg) -> me.getKey());
        boolean result3 = redisCache.invoke("a", (me, arg) -> me.exists());
        assertEquals("A1", result1);
        assertEquals("a", result2);
        assertTrue(result3);
        redisCache.invoke("a", (me, arg) -> {
            me.setValue("A2");
            return null;
        });
        String result4 = redisCache.invoke("a", (me, arg) -> me.getValue());
        assertEquals("A2", result4);
        redisCache.invoke("a", (me, arg) -> {
            me.remove();
            return null;
        });
        String result5 = redisCache.invoke("a", (me, arg) -> me.getValue());
        boolean result6 = redisCache.invoke("a", (me, arg) -> me.exists());
        assertNull(result5);
        assertFalse(result6);
    }

    @Test
    public void invokeTAllest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        redisCache.put("b","B1");
        redisCache.put("c","C1");
        Set<String> keys = new HashSet<>(Arrays.asList("a","b","c"));
        Map<String, EntryProcessorResult<String>> results = redisCache.invokeAll(keys, (me, arg) -> me.getValue());
        assertEquals("A1", results.get("a").get());
        assertEquals("B1", results.get("b").get());
        assertEquals("C1", results.get("c").get());
    }



}
