package org.obapanel.jedis.collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.params.SetParams;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.collections.MockOfJedisForList.unitTestEnabledForList;

public class MockOfJedisForListTest {


    private MockOfJedisForList mockOfJedis;

    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(unitTestEnabledForList());
        if (!unitTestEnabledForList()) return;
        mockOfJedis = new MockOfJedisForList();
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
        boolean t11 = mockOfJedis.isSetParamsNX(sp1);
        boolean t12 = Long.valueOf(1).equals(mockOfJedis.getExpireTimePX(sp1));
        SetParams sp2 = new SetParams();
        sp2.nx();
        boolean t21 = mockOfJedis.isSetParamsNX(sp2);
        boolean t22 = Long.valueOf(1).equals(mockOfJedis.getExpireTimePX(sp2));
        SetParams sp3 = new SetParams();
        sp3.px(1L);
        boolean t31 = mockOfJedis.isSetParamsNX(sp3);
        boolean t32 = Long.valueOf(1).equals(mockOfJedis.getExpireTimePX(sp3));
        SetParams sp4 = new SetParams();
        sp4.nx().px(1L);
        boolean t41 = mockOfJedis.isSetParamsNX(sp4);
        boolean t42 = Long.valueOf(1).equals(mockOfJedis.getExpireTimePX(sp4));

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
    }

    @Test
    public void testDataGetSetDel() {
        mockOfJedis.getJedis().set("a", "5", new SetParams().nx());
        assertEquals("5", mockOfJedis.getCurrentData().get("a"));
        mockOfJedis.getJedis().set("a", "7", new SetParams().nx());
        assertEquals("5", mockOfJedis.getCurrentData().get("a"));
        assertEquals("5", mockOfJedis.getJedis().get("a"));
        assertTrue(1L == mockOfJedis.getJedis().del("a"));
        assertNull(mockOfJedis.getCurrentData().get("a"));
    }

    @Test
    public void testDataDataToList() {
        mockOfJedis.put("data", new ArrayList<>(Arrays.asList("a","b","c")));
        assertTrue(mockOfJedis.dataToList("data") instanceof ArrayList);
        assertTrue(mockOfJedis.dataToList("data") != null);
        assertTrue(mockOfJedis.dataToList("data").get(1).equals("b"));
        assertTrue(mockOfJedis.dataToList("data").size() == 3);
        assertNotNull(mockOfJedis.dataToList("data2"));
        mockOfJedis.dataToList("data2").add("x");
        assertTrue(mockOfJedis.dataToList("data2").isEmpty());
        assertNotNull(mockOfJedis.dataToList("data2",true));
        assertTrue(mockOfJedis.dataToList("data2", true).isEmpty());
        mockOfJedis.dataToList("data2").add("x");
        assertFalse(mockOfJedis.dataToList("data2").isEmpty());
    }

    @Test
    public void testMockListLlen() {
        mockOfJedis.put("data1", new ArrayList<>(Arrays.asList("a","b","c")));
        long result1 = mockOfJedis.mockListLlen("data1");
        assertEquals(3L, result1);
        long result21 = mockOfJedis.mockListLlen("data2");
        assertEquals(0L, result21);
        assertTrue(mockOfJedis.dataToList("data2", true).isEmpty());
        long result22 = mockOfJedis.mockListLlen("data2");
        assertEquals(0L, result22);
        mockOfJedis.dataToList("data2").add("x");
        assertFalse(mockOfJedis.dataToList("data2").isEmpty());
        long result23 = mockOfJedis.mockListLlen("data2");
        assertEquals(1L, result23);
    }





}
