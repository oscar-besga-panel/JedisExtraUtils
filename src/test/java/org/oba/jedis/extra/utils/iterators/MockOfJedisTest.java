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

import static org.junit.Assert.*;
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
        mockOfJedis.getJedis().set("a", "A1", new SetParams());
        assertEquals("A1", mockOfJedis.getCurrentData().get("a"));
        mockOfJedis.getJedis().set("a", "A2", new SetParams());
        assertEquals("A2", mockOfJedis.getCurrentData().get("a"));
        mockOfJedis.getJedis().set("b", "B1", new SetParams().nx());
        assertEquals("B1", mockOfJedis.getCurrentData().get("b"));
        mockOfJedis.getJedis().set("b", "B2", new SetParams().nx());
        assertEquals("B1", mockOfJedis.getCurrentData().get("b"));
        mockOfJedis.getJedis().set("c", "C1", new SetParams().nx().px(500));
        assertEquals("C1", mockOfJedis.getCurrentData().get("c"));
        mockOfJedis.getJedis().set("c", "C2", new SetParams().nx().px(500));
        assertEquals("C1", mockOfJedis.getCurrentData().get("c"));
        Thread.sleep(1000);
        assertNull(mockOfJedis.getCurrentData().get("c"));
        assertEquals(1L, mockOfJedis.getJedis().del("a"));
        assertNull(mockOfJedis.getCurrentData().get("a"));
        assertEquals(0L, mockOfJedis.getJedis().del("a"));
    }

    @Test
    public void testMockScan() {
        mockOfJedis.getJedis().set("a", "A1", new SetParams());
        mockOfJedis.getJedis().set("b", "B1", new SetParams());
        mockOfJedis.getJedis().set("c", "C1", new SetParams());
        //ScanResult<String> result = mockOfJedis.getJedis().scan("", new ScanParams().match("*"));
        ScanResult<String> result = mockOfJedis.getJedis().scan("");
        assertEquals(ScanParams.SCAN_POINTER_START, result.getCursor());
        assertTrue( result.getResult().contains("a"));
        assertTrue( result.getResult().contains("b"));
        assertTrue( result.getResult().contains("c"));
    }

    @Test
    public void testMapDataInsertion() {
        mockOfJedis.getJedis().hset("map1", "a", "1");
        mockOfJedis.getJedis().hset("map1", "b", "2");
        mockOfJedis.getJedis().hset("map1", "c", "3");
        mockOfJedis.getJedis().hset("map2", "a", "10");
        assertEquals("1", mockOfJedis.getJedis().hget("map1", "a"));
        assertEquals("2", mockOfJedis.getJedis().hget("map1", "b"));
        assertEquals("3", mockOfJedis.getJedis().hget("map1", "c"));
        assertEquals("10", mockOfJedis.getJedis().hget("map2", "a"));
        assertNull(mockOfJedis.getJedis().hget("map2", "b"));
        assertNull(mockOfJedis.getJedis().hget("map3", "a"));
        mockOfJedis.getJedis().hdel("map1", "c");
        mockOfJedis.getJedis().hdel("map2", "c");
        assertNull(mockOfJedis.getJedis().hget("map1", "c"));
        assertEquals("1", mockOfJedis.getJedis().hget("map1", "a"));
        assertEquals("10", mockOfJedis.getJedis().hget("map2", "a"));
        mockOfJedis.getJedis().del("map2");
        assertNull(mockOfJedis.getJedis().hget("map2", "a"));
    }

    @Test
    public void testMapHscan() {
        mockOfJedis.getJedis().hset("map1", "a", "1");
        mockOfJedis.getJedis().hset("map1", "b", "2");
        mockOfJedis.getJedis().hset("map1", "c", "3");
        mockOfJedis.getJedis().hset("map2", "d", "10");
        ScanResult<Map.Entry<String, String>> scanResult = mockOfJedis.getJedis().hscan("map1", ScanParams.SCAN_POINTER_START, new ScanParams());
        Map<String, String> mapResult = new HashMap<>();
        scanResult.getResult().forEach( entry -> mapResult.put(entry.getKey(), entry.getValue()));
        assertEquals( ScanParams.SCAN_POINTER_START, scanResult.getCursor());
        assertEquals("1", mockOfJedis.getJedis().hget("map1", "a"));
        assertEquals("2", mockOfJedis.getJedis().hget("map1", "b"));
        assertEquals("3", mockOfJedis.getJedis().hget("map1", "c"));
        assertTrue(mapResult != null && !mapResult.isEmpty());
        assertEquals(3, mapResult.size());
        assertEquals("1", mapResult.get("a"));
        assertEquals("2", mapResult.get("b"));
        assertEquals("3", mapResult.get("c"));
        assertNull(mapResult.get("d"));
    }

    @Test
    public void testSetDataInsertion() {
        mockOfJedis.getJedis().sadd("set1", "a");
        mockOfJedis.getJedis().sadd("set1", "b");
        mockOfJedis.getJedis().sadd("set1", "c");
        mockOfJedis.getJedis().sadd("set2", "d");
        assertTrue(mockOfJedis.getJedis().sismember("set1","a"));
        assertTrue(mockOfJedis.getJedis().sismember("set1","b"));
        assertTrue(mockOfJedis.getJedis().sismember("set1","c"));
        assertFalse(mockOfJedis.getJedis().sismember("set1","d"));
        assertFalse(mockOfJedis.getJedis().sismember("set2","a"));
        assertTrue(mockOfJedis.getJedis().sismember("set2","d"));
        assertEquals(1L, mockOfJedis.getJedis().srem("set1", "c"));
        assertEquals(0L, mockOfJedis.getJedis().srem("set2", "c"));
        assertFalse(mockOfJedis.getJedis().sismember("set1","c"));
        mockOfJedis.getJedis().del("set2");
        assertFalse(mockOfJedis.getJedis().sismember("set2","d"));
    }

    @Test
    public void testSetSscan() {
        mockOfJedis.getJedis().sadd("set1", "a");
        mockOfJedis.getJedis().sadd("set1", "b");
        mockOfJedis.getJedis().sadd("set1", "c");
        mockOfJedis.getJedis().sadd("set2", "d");
        ScanResult<String> scanResult = mockOfJedis.getJedis().sscan("set1", ScanParams.SCAN_POINTER_START, new ScanParams());
        assertEquals(ScanParams.SCAN_POINTER_START, scanResult.getCursor());
        assertTrue( scanResult.getResult().contains("a"));
        assertTrue( scanResult.getResult().contains("b"));
        assertTrue( scanResult.getResult().contains("c"));
        assertEquals( 3, scanResult.getResult().size());
    }

    @Test
    public void testZSetDataInsertion() {
        mockOfJedis.getJedis().zadd("zset1", 1.0, "a");
        mockOfJedis.getJedis().zadd("zset1", 2.0, "b");
        mockOfJedis.getJedis().zadd("zset1", 3.0, "c");
        mockOfJedis.getJedis().zadd("zset2", 4.0, "d");
        assertEquals(Double.valueOf(1.0), mockOfJedis.getJedis().zscore("zset1", "a"));
        assertEquals(Double.valueOf(2.0), mockOfJedis.getJedis().zscore("zset1", "b"));
        assertEquals(Double.valueOf(3.0), mockOfJedis.getJedis().zscore("zset1", "c"));
        assertNull( mockOfJedis.getJedis().zscore("zset1", "d"));
        assertEquals(Double.valueOf(4.0), mockOfJedis.getJedis().zscore("zset2", "d"));
        assertNull( mockOfJedis.getJedis().zscore("zset2", "a"));
        mockOfJedis.getJedis().zrem("zset1","b");
        assertNull( mockOfJedis.getJedis().zscore("zset1", "b"));
        mockOfJedis.getJedis().del("zset2");
        assertNull( mockOfJedis.getJedis().zscore("zset2", "d"));
    }

    @Test
    public void testZSetZscan() {
        mockOfJedis.getJedis().zadd("zset1", 1.0, "a");
        mockOfJedis.getJedis().zadd("zset1", 2.0, "b");
        mockOfJedis.getJedis().zadd("zset1", 3.0, "c");
        mockOfJedis.getJedis().zadd("zset2", 4.0, "d");
        ScanResult<Tuple> scanResult = mockOfJedis.getJedis().zscan("zset1", ScanParams.SCAN_POINTER_START, new ScanParams());
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

