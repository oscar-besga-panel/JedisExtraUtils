package org.obapanel.jedis.collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.AbstractMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.collections.MockOfJedisForList.unitTestEnabledForList;

public class MockOfJedisForMapTest {


    private MockOfJedisForMap mockOfJedis;

    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(unitTestEnabledForList());
        if (!unitTestEnabledForList()) return;
        mockOfJedis = new MockOfJedisForMap();
    }

    @After
    public void tearDown() {
        if (mockOfJedis != null) {
            mockOfJedis.clearData();
        }
    }

    @Test
    public void testDataSize() throws InterruptedException {
        mockOfJedis.mockHset("map1","a1","11");
        long result1 = mockOfJedis.mockHlen("map1");
        mockOfJedis.mockHset("map1","a2","12");
        mockOfJedis.mockHset("map1","a3","13");
        long result2 = mockOfJedis.mockHlen("map1");
        assertEquals(1L, result1);
        assertEquals(3L, result2);
    }



    @Test
    public void testDataInsertion() throws InterruptedException {
        long result = mockOfJedis.mockHset("map1","a1","11");
        boolean exists1 = mockOfJedis.mockExists("map1");
        boolean exists2 = mockOfJedis.mockExists("map2");
        boolean hexists1 = mockOfJedis.mockHexists("map1", "a1");
        boolean hexists2 = mockOfJedis.mockHexists("map1", "a2");
        boolean hexists3 = mockOfJedis.mockHexists("map2", "a2");
        String result1 = mockOfJedis.mockHget("map1", "a1");
        String result2 = mockOfJedis.mockHget("map1", "b1");
        String result3 = mockOfJedis.mockHget("map2", "a1");
        assertEquals(1L, result);
        assertNotNull(mockOfJedis.getCurrentData().get("map1"));
        assertNull(mockOfJedis.getCurrentData().get("map2"));
        assertTrue(exists1);
        assertFalse(exists2);
        assertTrue(hexists1);
        assertFalse(hexists2);
        assertFalse(hexists3);
        assertEquals("11", result1);
        assertNull(result2);
        assertNull(result3);
        long resultd1 = mockOfJedis.mockDelete("map1");
        long resultd2 = mockOfJedis.mockDelete("map2");
        assertEquals(1L, resultd1);
        assertEquals(0L, resultd2);
        assertNull(mockOfJedis.getCurrentData().get("map1"));
    }

    @Test
    public void testDataInsertionTransaction() throws InterruptedException {
        Response<Long> result = mockOfJedis.mockTransactionHset("map1","a1","11");
        boolean exists1 = mockOfJedis.mockExists("map1");
        boolean exists2 = mockOfJedis.mockExists("map2");
        Response<String> result1 = mockOfJedis.mockTransactionHget("map1", "a1");
        Response<String> result2 = mockOfJedis.mockTransactionHget("map1", "b1");
        Response<String> result3 = mockOfJedis.mockTransactionHget("map2", "a1");
        assertEquals(Long.valueOf(1L), result.get());
        assertNotNull(mockOfJedis.getCurrentData().get("map1"));
        assertNull(mockOfJedis.getCurrentData().get("map2"));
        assertTrue(exists1);
        assertFalse(exists2);
        assertEquals("11", result1.get());
        assertNull(result2.get());
        assertNull(result3.get());
    }


    @Test
    public void testDataDeletion() throws InterruptedException {
        mockOfJedis.mockHset("map1","a1","11");
        mockOfJedis.mockHset("map3","a3","33");
        Long result11 = mockOfJedis.mockHdel("map1","a1");
        Long result12 = mockOfJedis.mockHdel("map1","a2");
        Long result13 = mockOfJedis.mockHdel("map1","a1");
        Long result14 = mockOfJedis.mockHdel("map2","a1");
        Response<Long> result21 = mockOfJedis.mockTransactionHdel("map3", "a3");
        Response<Long> result22 = mockOfJedis.mockTransactionHdel("map3", "a4");
        Response<Long> result23 = mockOfJedis.mockTransactionHdel("map3", "a3");
        Response<Long> result24 = mockOfJedis.mockTransactionHdel("map4", "a3");
        assertEquals(Long.valueOf(1L), result11);
        assertEquals(Long.valueOf(0L), result12);
        assertEquals(Long.valueOf(0L), result13);
        assertEquals(Long.valueOf(0L), result14);
        assertEquals(Long.valueOf(1L), result21.get());
        assertEquals(Long.valueOf(0L), result22.get());
        assertEquals(Long.valueOf(0L), result23.get());
        assertEquals(Long.valueOf(0L), result24.get());
    }

    @Test
    public void testScan() {
        mockOfJedis.mockHset("map1","a1","11");
        mockOfJedis.mockHset("map1","a2","12");
        mockOfJedis.mockHset("map1","a3","13");
        ScanResult<Map.Entry<String, String>> result =  mockOfJedis.mockHscan("map1", ScanParams.SCAN_POINTER_START,new ScanParams());
        assertEquals( ScanParams.SCAN_POINTER_START, result.getCursor());
        assertEquals(3 , result.getResult().size());
        assertTrue(mockOfJedis.mockHlen("map1") == result.getResult().size());
        assertTrue(result.getResult().contains(new AbstractMap.SimpleImmutableEntry<>("a1","11")));
        assertTrue(result.getResult().contains(new AbstractMap.SimpleImmutableEntry<>("a2","12")));
        assertTrue(result.getResult().contains(new AbstractMap.SimpleImmutableEntry<>("a3","13")));
    }

}
