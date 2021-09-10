package org.obapanel.jedis.collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.collections.MockOfJedisForList.unitTestEnabledForList;

public class MockOfJedisForSetTest {

    private MockOfJedisForSet mockOfJedis;

    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(unitTestEnabledForList());
        if (!unitTestEnabledForList()) return;
        mockOfJedis = new MockOfJedisForSet();
    }

    @After
    public void tearDown() {
        if (mockOfJedis != null) {
            mockOfJedis.clearData();
        }
    }

    @Test
    public void testMockExists() {
        JedisSet jedisSet = new JedisSet(mockOfJedis.getJedisPool(), "set1");
        jedisSet.add("a");
        assertTrue(mockOfJedis.mockExists("set1"));
        assertEquals( Long.valueOf(1L), mockOfJedis.mockScard("set1"));
    }

    @Test
    public void testMockDel() {
        JedisSet jedisSet = new JedisSet(mockOfJedis.getJedisPool(), "set1");
        jedisSet.add("a");
        assertTrue(mockOfJedis.mockExists("set1"));
        assertEquals( Long.valueOf(1L), mockOfJedis.mockScard("set1"));
        Long result = mockOfJedis.mockDel("set1");
        assertEquals(Long.valueOf(1L), result);
        assertFalse(mockOfJedis.mockExists("set1"));
        assertEquals( Long.valueOf(0L), mockOfJedis.mockScard("set1"));
    }

    @Test
    public void testMockSaddScardSismember() {
        JedisSet jedisSet = new JedisSet(mockOfJedis.getJedisPool(), "set1");
        jedisSet.add("a");
        assertEquals( Long.valueOf(1L), mockOfJedis.mockScard("set1"));
        assertTrue(mockOfJedis.mockSismember("set1", "a"));
        assertFalse(mockOfJedis.mockSismember("set1", "b"));
        assertFalse(mockOfJedis.mockSismember("set1", "c"));
        Long result1 = mockOfJedis.mockSadd("set1","b");
        assertEquals(Long.valueOf(1L), result1);
        assertTrue(jedisSet.contains("b"));
        assertEquals( Long.valueOf(2L), mockOfJedis.mockScard("set1"));
        assertTrue(mockOfJedis.mockSismember("set1", "b"));
        assertTrue(mockOfJedis.mockSismember("set1", "a"));
        assertTrue(mockOfJedis.mockSismember("set1", "b"));
        assertFalse(mockOfJedis.mockSismember("set1", "c"));
        Long result2 = mockOfJedis.mockSadd("set1", new String[]{"c", "d"});
        assertEquals(Long.valueOf(2L), result2);
        assertTrue(jedisSet.contains("c"));
        assertTrue(jedisSet.contains("d"));
        assertTrue(mockOfJedis.mockSismember("set1", "c"));
        assertTrue(mockOfJedis.mockSismember("set1", "d"));
        assertEquals( Long.valueOf(4L), mockOfJedis.mockScard("set1"));
        assertTrue(mockOfJedis.mockSismember("set1", "a"));
        assertTrue(mockOfJedis.mockSismember("set1", "b"));
        assertTrue(mockOfJedis.mockSismember("set1", "c"));
        Long result3 = mockOfJedis.mockSadd("set1", new String[]{"e", "a"});
        assertEquals(Long.valueOf(1L), result3);
        assertTrue(jedisSet.contains("e"));
        assertEquals( Long.valueOf(5L), mockOfJedis.mockScard("set1"));
        Long result4 = mockOfJedis.mockSadd("set1", new String[]{"b", "a"});
        assertEquals(Long.valueOf(0L), result4);
        assertTrue(jedisSet.contains("e"));
        assertEquals( Long.valueOf(5L), mockOfJedis.mockScard("set1"));
    }


    @Test
    public void testMockSscan() {
        mockOfJedis.mockSadd("set3", new String[]{"a", "b", "c"});
        ScanResult<String> scanResult = mockOfJedis.mockSscan("set3", ScanParams.SCAN_POINTER_START, new ScanParams());
        assertEquals( Long.valueOf(3L), mockOfJedis.mockScard("set3"));
        assertTrue(mockOfJedis.mockSismember("set3", "a"));
        assertTrue(mockOfJedis.mockSismember("set3", "b"));
        assertTrue(mockOfJedis.mockSismember("set3", "c"));
        assertEquals( mockOfJedis.mockScard("set3"), Long.valueOf(scanResult.getResult().size()));
        assertTrue(scanResult.getResult().contains("a"));
        assertTrue(scanResult.getResult().contains("b"));
        assertTrue(scanResult.getResult().contains("c"));
    }

    @Test
    public void testMockSRem() {
        mockOfJedis.mockSadd("set5", new String[]{"a", "b", "c", "d", "e", "f", "g"});
        assertEquals( Long.valueOf(7L), mockOfJedis.mockScard("set5"));
        Long result1 = mockOfJedis.mockSrem("set5", "a");
        assertEquals(Long.valueOf(1L), result1);
        assertEquals( Long.valueOf(6L), mockOfJedis.mockScard("set5"));
        Long result2 = mockOfJedis.mockSrem("set5", new String[]{"b", "c"});
        assertEquals(Long.valueOf(2L), result2);
        assertEquals( Long.valueOf(4L), mockOfJedis.mockScard("set5"));
        Long result3 = mockOfJedis.mockSrem("set5", new String[]{"d", "a", "b"});
        assertEquals(Long.valueOf(1L), result3);
        assertEquals( Long.valueOf(3L), mockOfJedis.mockScard("set5"));
        Long result4 = mockOfJedis.mockSrem("set5", new String[]{"c", "a", "b"});
        assertEquals(Long.valueOf(0L), result4);
        assertEquals( Long.valueOf(3L), mockOfJedis.mockScard("set5"));
    }

}
