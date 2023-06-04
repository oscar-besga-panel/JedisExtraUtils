package org.oba.jedis.extra.utils.cache;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.oba.jedis.extra.utils.utils.SimpleEntry;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBase;

import java.util.*;

import static org.junit.Assert.*;
import static org.oba.jedis.extra.utils.cache.MockOfJedisForSimpleCache.unitTestEnabledForSimpleCache;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Transaction.class, TransactionBase.class })
public class SimpleCacheTest {


    private MockOfJedisForSimpleCache mockOfJedisForsimpleCache;


    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(unitTestEnabledForSimpleCache());
        if (!unitTestEnabledForSimpleCache()) return;
        mockOfJedisForsimpleCache = new MockOfJedisForSimpleCache();
    }

    @After
    public void tearDown() {
        if (mockOfJedisForsimpleCache != null) {
            mockOfJedisForsimpleCache.clearData();
        }
    }

    SimpleCache createNewCache() {
        return createNewCache(3_600_000);
    }

    SimpleCache createNewCache(long timeOut) {
        String name = "cache:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        return new SimpleCache(mockOfJedisForsimpleCache.getJedisPool(), name, timeOut);
    }


    @Test
    public void putAndGetTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        String result = simpleCache.get("a");
        assertEquals("A1", result);
        assertEquals("A1", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
    }

    @Test
    public void getAllTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        simpleCache.put("b", "B1");
        simpleCache.put("c", "C1");
        Set<String> keys = new HashSet<>(Arrays.asList(new String[]{"a", "b", "c"}));
        Map<String, String> results = simpleCache.getAll(keys);
        assertEquals(3, results.size());
        assertEquals("A1", results.get("a"));
        assertEquals("B1", results.get("b"));
        assertEquals("C1", results.get("c"));
        assertEquals("A1", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
    }

    @Test
    public void containsKeyTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        simpleCache.put("b", "B1");
        assertTrue(simpleCache.containsKey("a"));
        assertTrue(mockOfJedisForsimpleCache.getJedis().exists(simpleCache.getName() + ":a"));
        assertTrue(mockOfJedisForsimpleCache.getCurrentData().containsKey(simpleCache.getName() + ":a"));
        assertTrue(simpleCache.containsKey("b"));
        assertTrue(mockOfJedisForsimpleCache.getJedis().exists(simpleCache.getName() + ":b"));
        assertTrue(mockOfJedisForsimpleCache.getCurrentData().containsKey(simpleCache.getName() + ":b"));
        assertFalse(simpleCache.containsKey("c"));
        assertFalse(mockOfJedisForsimpleCache.getJedis().exists(simpleCache.getName() + ":c"));
        assertFalse(mockOfJedisForsimpleCache.getCurrentData().containsKey(simpleCache.getName() + ":c"));
    }

    @Test
    public void putTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        Set<String> keys = new HashSet<>(Arrays.asList(new String[]{"a", "b", "c"}));
        Map<String, String> results = simpleCache.getAll(keys);
        assertEquals(1, results.size());
        assertEquals("A1", simpleCache.get("a"));
        assertEquals("A1", results.get("a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
    }

    @Test
    public void putWithTimeoutTest() throws InterruptedException {
        SimpleCache simpleCache = createNewCache(500);
        simpleCache.put("a", "A1");
        String result1 = simpleCache.get("a");
        Thread.sleep(700);
        String result2 = simpleCache.get("a");
        assertEquals("A1", result1);
        assertNull(result2);
    }

    @Test
    public void getAndPutTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        String previous = simpleCache.getAndPut("a", "A2");
        assertEquals("A1", previous);
        assertEquals("A2", simpleCache.get("a"));
        assertEquals("A2", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertEquals("A2", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
    }

    @Test
    public void putAllTest() {
        SimpleCache simpleCache = createNewCache();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        simpleCache.putAll(data);
        assertEquals("A1", simpleCache.get("a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
        assertEquals("B1", simpleCache.get("b"));
        assertEquals("B1", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":b"));
        assertEquals("B1", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":b"));
    }

    @Test
    public void putIfAbsentTest() {
        SimpleCache simpleCache = createNewCache();
        boolean result1 = simpleCache.putIfAbsent("a", "A1");
        boolean result2 = simpleCache.putIfAbsent("a", "A2");
        assertEquals("A1", simpleCache.get("a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
        assertTrue(result1);
        assertFalse(result2);
        simpleCache.put("a", "A2");
        assertEquals("A2", simpleCache.get("a"));
        assertEquals("A2", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertEquals("A2", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
    }

    @Test
    public void removeTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        assertEquals("A1", simpleCache.get("a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
        simpleCache.remove("a");
        assertNull(simpleCache.get("a"));
        assertNull(mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertNull(mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
    }

    @Test
    public void removeTest2() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        assertEquals("A1", simpleCache.get("a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
        simpleCache.remove("a", "A2");
        assertEquals("A1", simpleCache.get("a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
        simpleCache.remove("a", "A1");
        assertNull(simpleCache.get("a"));
        assertNull(mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertNull(mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
    }

    @Test
    public void getAndRemoveTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        assertEquals("A1", simpleCache.get("a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
        String removed = simpleCache.getAndRemove("a");
        assertNull(simpleCache.get("a"));
        assertNull(mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertNull(mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
        assertEquals("A1", removed);
    }

    @Test
    public void replaceTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        assertEquals("A1", simpleCache.get("a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
        boolean replaced1 = simpleCache.replace("a", "A1", "A2");
        assertEquals("A2", simpleCache.get("a"));
        assertEquals("A2", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertEquals("A2", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
        assertTrue(replaced1);
        boolean replaced2 = simpleCache.replace("a", "A3", "A4");
        assertEquals("A2", simpleCache.get("a"));
        assertEquals("A2", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertEquals("A2", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
        assertFalse(replaced2);
    }

    @Test
    public void replace2Test() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        assertEquals("A1", simpleCache.get("a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
        boolean replaced1 = simpleCache.replace("a", "A2");
        assertEquals("A2", simpleCache.get("a"));
        assertEquals("A2", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertEquals("A2", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
        assertTrue(replaced1);
        boolean replaced2 = simpleCache.replace("b", "B1");
        assertNull(simpleCache.get("b"));
        assertNull(mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":b"));
        assertNull(mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":b"));
        assertFalse(replaced2);
    }

    @Test
    public void getAndReplaceTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        assertEquals("A1", simpleCache.get("a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertEquals("A1", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
        String replaced = simpleCache.getAndReplace("a", "A2");
        assertEquals("A2", simpleCache.get("a"));
        assertEquals("A2", mockOfJedisForsimpleCache.getJedis().get(simpleCache.getName() + ":a"));
        assertEquals("A2", mockOfJedisForsimpleCache.getCurrentData().get(simpleCache.getName() + ":a"));
        assertEquals("A1", replaced);
    }

    @Test
    public void removeAllKeysTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        simpleCache.put("b", "B1");
        simpleCache.put("c", "C1");
        simpleCache.put("d", "D1");
        simpleCache.put("e", "E1");
        Set<String> toRemove = new HashSet<>(Arrays.asList("b", "d", "e"));
        simpleCache.removeAll(toRemove);
        assertTrue(simpleCache.containsKey("a"));
        assertFalse(simpleCache.containsKey("b"));
        assertTrue(simpleCache.containsKey("c"));
        assertFalse(simpleCache.containsKey("d"));
        assertFalse(simpleCache.containsKey("e"));
    }

    @Test
    public void removeAllTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        simpleCache.put("b", "B1");
        simpleCache.put("c", "C1");
        simpleCache.put("d", "D1");
        simpleCache.put("e", "E1");
        simpleCache.removeAll();
        assertFalse(simpleCache.containsKey("a"));
        assertFalse(simpleCache.containsKey("b"));
        assertFalse(simpleCache.containsKey("c"));
        assertFalse(simpleCache.containsKey("d"));
        assertFalse(simpleCache.containsKey("e"));
    }

    @Test
    public void clearTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        simpleCache.put("b", "B1");
        simpleCache.put("c", "C1");
        simpleCache.put("d", "D1");
        simpleCache.put("e", "E1");
        simpleCache.clear();
        assertFalse(simpleCache.containsKey("a"));
        assertFalse(simpleCache.containsKey("b"));
        assertFalse(simpleCache.containsKey("c"));
        assertFalse(simpleCache.containsKey("d"));
        assertFalse(simpleCache.containsKey("e"));
    }

    @Test
    public void clearEmptyTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.clear();
        assertTrue(simpleCache.keys().isEmpty());
    }


    @Test
    public void getIteratorTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        simpleCache.put("b", "B1");
        simpleCache.put("c", "C1");
        Iterator<Map.Entry<String,String>> meit = simpleCache.iterator();
        assertNotNull(meit);
        while (meit.hasNext()) {
            Map.Entry<String,String> me = meit.next();
            assertTrue(Arrays.asList("a","b","c").contains(me.getKey()));
            assertTrue(Arrays.asList("A1","B1","C1").contains(me.getValue()));
        }
    }

    @Test
    public void getKeysIteratorTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        simpleCache.put("b", "B1");
        simpleCache.put("c", "C1");
        Iterator<String> kit = simpleCache.keysIterator();
        assertNotNull(kit);
        while (kit.hasNext()) {
            String key = kit.next();
            assertTrue(Arrays.asList("a","b","c").contains(key));
            assertTrue(Arrays.asList("A1","B1","C1").contains(simpleCache.get(key)));
        }
    }

    @Test
    public void keysTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        simpleCache.put("b", "B1");
        simpleCache.put("c", "C1");
        List<String> keys = simpleCache.keys();
        for(String key: keys){
            assertTrue(Arrays.asList("a","b","c").contains(key));
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void keysErrorTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        List<String> keys = simpleCache.keys();
        keys.add("b");
    }


    @Test
    public void asListTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        simpleCache.put("b", "B1");
        simpleCache.put("c", "C1");
        List<Map.Entry<String,String>> entries = simpleCache.asList();
        for(Map.Entry<String,String> entry: entries){
            assertTrue(Arrays.asList("a","b","c").contains(entry.getKey()));
            assertTrue(Arrays.asList("A1","B1","C1").contains(entry.getValue()));
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void asListErrorTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        List<Map.Entry<String,String>> entries = simpleCache.asList();
        entries.add(new SimpleEntry("b","B1"));
    }

    @Test
    public void asMapTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        simpleCache.put("b", "B1");
        simpleCache.put("c", "C1");
        Map<String,String> map = simpleCache.asMap();
        for(Map.Entry<String,String> entry: map.entrySet()){
            assertTrue(Arrays.asList("a","b","c").contains(entry.getKey()));
            assertTrue(Arrays.asList("A1","B1","C1").contains(entry.getValue()));
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void asMapErrorTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        Map<String,String> map = simpleCache.asMap();
        map.put("b","B1");
    }

    @Test
    public void doNotClose() {
        SimpleCache simpleCache = createNewCache();
        assertFalse(simpleCache.isClosed());
        simpleCache.get("a");
    }

    @Test(expected = IllegalStateException.class)
    public void doClose() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.close();
        assertTrue(simpleCache.isClosed());
        simpleCache.get("a");
    }


}
