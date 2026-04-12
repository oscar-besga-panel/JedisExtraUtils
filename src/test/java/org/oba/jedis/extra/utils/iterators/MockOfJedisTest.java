package org.oba.jedis.extra.utils.iterators;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.oba.jedis.extra.utils.test.TestingUtils.extractSetParamsExpireTimePX;
import static org.oba.jedis.extra.utils.test.TestingUtils.isSetParamsNX;

public class MockOfJedisTest {


    private MockOfJedis mockOfJedis;

    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(MockOfJedis.unitTestEnabled());
        if (!MockOfJedis.unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
    }

    @After
    public void tearDown() {
        if (mockOfJedis != null) {
            mockOfJedis.clearData();
        }
    }

    @Test
    public void testParams() {
        SetParams sp1 = new SetParams();
        boolean t11 = isSetParamsNX(sp1);
        boolean t12 = Long.valueOf(1).equals(extractSetParamsExpireTimePX(sp1));
        SetParams sp2 = new SetParams();
        sp2.nx();
        boolean t21 = isSetParamsNX(sp2);
        boolean t22 = Long.valueOf(1).equals(extractSetParamsExpireTimePX(sp2));
        SetParams sp3 = new SetParams();
        sp3.px(1L);
        boolean t31 = isSetParamsNX(sp3);
        boolean t32 = Long.valueOf(1).equals(extractSetParamsExpireTimePX(sp3));
        SetParams sp4 = new SetParams();
        sp4.nx().px(1L);
        boolean t41 = isSetParamsNX(sp4);
        boolean t42 = Long.valueOf(1).equals(extractSetParamsExpireTimePX(sp4));

        boolean finalResult = !t11 && !t12 && t21 && !t22 && !t31 && t32 && t41 && t42;
        assertTrue(finalResult);
    }

    @Test
    public void testDataInsertion() throws InterruptedException {
        mockOfJedis.getRedisClient().set("a", "A1", new SetParams());
        assertEquals("A1", mockOfJedis.getCurrentData().get("a"));
        mockOfJedis.getRedisClient().set("a", "A2", new SetParams());
        assertEquals("A2", mockOfJedis.getCurrentData().get("a"));
        mockOfJedis.getRedisClient().set("b", "B1", new SetParams().nx());
        assertEquals("B1", mockOfJedis.getCurrentData().get("b"));
        mockOfJedis.getRedisClient().set("b", "B2", new SetParams().nx());
        assertEquals("B1", mockOfJedis.getCurrentData().get("b"));
        mockOfJedis.getRedisClient().set("c", "C1", new SetParams().nx().px(500));
        assertEquals("C1", mockOfJedis.getCurrentData().get("c"));
        mockOfJedis.getRedisClient().set("c", "C2", new SetParams().nx().px(500));
        assertEquals("C1", mockOfJedis.getCurrentData().get("c"));
        Thread.sleep(1000);
        assertNull(mockOfJedis.getCurrentData().get("c"));
        assertEquals(1L, mockOfJedis.getRedisClient().del("a"));
        assertNull(mockOfJedis.getCurrentData().get("a"));
        assertEquals(0L, mockOfJedis.getRedisClient().del("a"));
    }

    @Test
    public void testMockScan() {
        mockOfJedis.getRedisClient().set("a", "A1", new SetParams());
        mockOfJedis.getRedisClient().set("b", "B1", new SetParams());
        mockOfJedis.getRedisClient().set("c", "C1", new SetParams());
        //ScanResult<String> result = mockOfJedis.getRedisClient().scan("", new ScanParams().match("*"));
        ScanResult<String> result = mockOfJedis.getRedisClient().scan("");
        assertEquals(ScanParams.SCAN_POINTER_START, result.getCursor());
        assertTrue( result.getResult().contains("a"));
        assertTrue( result.getResult().contains("b"));
        assertTrue( result.getResult().contains("c"));
    }

    @Test
    public void testMapDataInsertion() {
        mockOfJedis.getRedisClient().hset("map1", "a", "1");
        mockOfJedis.getRedisClient().hset("map1", "b", "2");
        mockOfJedis.getRedisClient().hset("map1", "c", "3");
        mockOfJedis.getRedisClient().hset("map2", "a", "10");
        assertEquals("1", mockOfJedis.getRedisClient().hget("map1", "a"));
        assertEquals("2", mockOfJedis.getRedisClient().hget("map1", "b"));
        assertEquals("3", mockOfJedis.getRedisClient().hget("map1", "c"));
        assertEquals("10", mockOfJedis.getRedisClient().hget("map2", "a"));
        assertNull(mockOfJedis.getRedisClient().hget("map2", "b"));
        assertNull(mockOfJedis.getRedisClient().hget("map3", "a"));
        mockOfJedis.getRedisClient().hdel("map1", "c");
        mockOfJedis.getRedisClient().hdel("map2", "c");
        assertNull(mockOfJedis.getRedisClient().hget("map1", "c"));
        assertEquals("1", mockOfJedis.getRedisClient().hget("map1", "a"));
        assertEquals("10", mockOfJedis.getRedisClient().hget("map2", "a"));
        mockOfJedis.getRedisClient().del("map2");
        assertNull(mockOfJedis.getRedisClient().hget("map2", "a"));
    }

    @Test
    public void testMapHscan() {
        mockOfJedis.getRedisClient().hset("map1", "a", "1");
        mockOfJedis.getRedisClient().hset("map1", "b", "2");
        mockOfJedis.getRedisClient().hset("map1", "c", "3");
        mockOfJedis.getRedisClient().hset("map2", "d", "10");
        ScanResult<Map.Entry<String, String>> scanResult = mockOfJedis.getRedisClient().hscan("map1", ScanParams.SCAN_POINTER_START, new ScanParams());
        Map<String, String> mapResult = new HashMap<>();
        scanResult.getResult().forEach( entry -> mapResult.put(entry.getKey(), entry.getValue()));
        assertEquals( ScanParams.SCAN_POINTER_START, scanResult.getCursor());
        assertEquals("1", mockOfJedis.getRedisClient().hget("map1", "a"));
        assertEquals("2", mockOfJedis.getRedisClient().hget("map1", "b"));
        assertEquals("3", mockOfJedis.getRedisClient().hget("map1", "c"));
        assertTrue(mapResult != null && !mapResult.isEmpty());
        assertEquals(3, mapResult.size());
        assertEquals("1", mapResult.get("a"));
        assertEquals("2", mapResult.get("b"));
        assertEquals("3", mapResult.get("c"));
        assertNull(mapResult.get("d"));
    }

    @Test
    public void testSetDataInsertion() {
        mockOfJedis.getRedisClient().sadd("set1", "a");
        mockOfJedis.getRedisClient().sadd("set1", "b");
        mockOfJedis.getRedisClient().sadd("set1", "c");
        mockOfJedis.getRedisClient().sadd("set2", "d");
        assertTrue(mockOfJedis.getRedisClient().sismember("set1","a"));
        assertTrue(mockOfJedis.getRedisClient().sismember("set1","b"));
        assertTrue(mockOfJedis.getRedisClient().sismember("set1","c"));
        assertFalse(mockOfJedis.getRedisClient().sismember("set1","d"));
        assertFalse(mockOfJedis.getRedisClient().sismember("set2","a"));
        assertTrue(mockOfJedis.getRedisClient().sismember("set2","d"));
        assertEquals(1L, mockOfJedis.getRedisClient().srem("set1", "c"));
        assertEquals(0L, mockOfJedis.getRedisClient().srem("set2", "c"));
        assertFalse(mockOfJedis.getRedisClient().sismember("set1","c"));
        mockOfJedis.getRedisClient().del("set2");
        assertFalse(mockOfJedis.getRedisClient().sismember("set2","d"));
    }

    @Test
    public void testSetSscan() {
        mockOfJedis.getRedisClient().sadd("set1", "a");
        mockOfJedis.getRedisClient().sadd("set1", "b");
        mockOfJedis.getRedisClient().sadd("set1", "c");
        mockOfJedis.getRedisClient().sadd("set2", "d");
        ScanResult<String> scanResult = mockOfJedis.getRedisClient().sscan("set1", ScanParams.SCAN_POINTER_START, new ScanParams());
        assertEquals(ScanParams.SCAN_POINTER_START, scanResult.getCursor());
        assertTrue( scanResult.getResult().contains("a"));
        assertTrue( scanResult.getResult().contains("b"));
        assertTrue( scanResult.getResult().contains("c"));
        assertEquals( 3, scanResult.getResult().size());
    }

    @Test
    public void testZSetDataInsertion() {
        mockOfJedis.getRedisClient().zadd("zset1", 1.0, "a");
        mockOfJedis.getRedisClient().zadd("zset1", 2.0, "b");
        mockOfJedis.getRedisClient().zadd("zset1", 3.0, "c");
        mockOfJedis.getRedisClient().zadd("zset2", 4.0, "d");
        assertEquals(Double.valueOf(1.0), mockOfJedis.getRedisClient().zscore("zset1", "a"));
        assertEquals(Double.valueOf(2.0), mockOfJedis.getRedisClient().zscore("zset1", "b"));
        assertEquals(Double.valueOf(3.0), mockOfJedis.getRedisClient().zscore("zset1", "c"));
        assertNull( mockOfJedis.getRedisClient().zscore("zset1", "d"));
        assertEquals(Double.valueOf(4.0), mockOfJedis.getRedisClient().zscore("zset2", "d"));
        assertNull( mockOfJedis.getRedisClient().zscore("zset2", "a"));
        mockOfJedis.getRedisClient().zrem("zset1","b");
        assertNull( mockOfJedis.getRedisClient().zscore("zset1", "b"));
        mockOfJedis.getRedisClient().del("zset2");
        assertNull( mockOfJedis.getRedisClient().zscore("zset2", "d"));
    }

    @Test
    public void testZSetZscan() {
        mockOfJedis.getRedisClient().zadd("zset1", 1.0, "a");
        mockOfJedis.getRedisClient().zadd("zset1", 2.0, "b");
        mockOfJedis.getRedisClient().zadd("zset1", 3.0, "c");
        mockOfJedis.getRedisClient().zadd("zset2", 4.0, "d");
        ScanResult<Tuple> scanResult = mockOfJedis.getRedisClient().zscan("zset1", ScanParams.SCAN_POINTER_START, new ScanParams());
        assertEquals(ScanParams.SCAN_POINTER_START, scanResult.getCursor());
        AtomicInteger count = new AtomicInteger(0);
        AtomicReference<Double> score = new AtomicReference<>(Double.valueOf(0.0));
        StringBuilder result = new StringBuilder();
        scanResult.getResult().forEach( tuple -> {
            count.incrementAndGet();
            score.accumulateAndGet(tuple.getScore(), (x,y) -> x + y);
            result.append(tuple.getElement());
        });
        assertTrue( result.indexOf("a") >= 0);
        assertTrue( result.indexOf("b") >= 0);
        assertTrue( result.indexOf("c") >= 0);
        assertEquals( 3, count.get());
        assertEquals( Double.valueOf(6.0), score.get());
    }


}

