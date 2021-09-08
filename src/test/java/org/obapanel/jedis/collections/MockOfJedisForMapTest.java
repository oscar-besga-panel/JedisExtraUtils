package org.obapanel.jedis.collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

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
    public void testDataInsertion() throws InterruptedException {
        long result = mockOfJedis.mockHset("map1","a1","11");
        boolean exists1 = mockOfJedis.mockExists("map1");
        boolean exists2 = mockOfJedis.mockExists("map2");
        String result1 = mockOfJedis.mockHget("map1", "a1");
        String result2 = mockOfJedis.mockHget("map1", "b1");
        String result3 = mockOfJedis.mockHget("map2", "a1");
        assertEquals(1L, result);
        assertNotNull(mockOfJedis.getCurrentData().get("map1"));
        assertNull(mockOfJedis.getCurrentData().get("map2"));
        assertTrue(exists1);
        assertFalse(exists2);
        assertEquals("11", result1);
        assertNull(result2);
        assertNull(result3);
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
}
