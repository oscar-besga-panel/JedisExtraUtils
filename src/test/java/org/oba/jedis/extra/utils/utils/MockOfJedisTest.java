package org.oba.jedis.extra.utils.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.resps.ScanResult;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
    public void testExists() {
        mockOfJedis.getJedis().set("a", "A1", new SetParams());
        assertEquals("A1", mockOfJedis.getCurrentData().get("a"));
        assertNull(mockOfJedis.getCurrentData().get("b"));
        assertTrue( mockOfJedis.getJedis().exists("a"));
        assertFalse( mockOfJedis.getJedis().exists("b"));
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
    public void testDataGetSetIncrDel() {
        mockOfJedis.getJedis().set("a", "5", new SetParams().nx());
        assertEquals("5", mockOfJedis.getCurrentData().get("a"));
        mockOfJedis.getJedis().set("a", "7", new SetParams().nx());
        assertEquals("5", mockOfJedis.getCurrentData().get("a"));
        assertEquals("5", mockOfJedis.getJedis().get("a"));
        assertEquals(1L, mockOfJedis.getJedis().del("a"));
        assertNull(mockOfJedis.getCurrentData().get("a"));
    }

    @Test
    public void testDataScan() throws InterruptedException {
        mockOfJedis.getJedis().set("a", "A1", new SetParams());
        mockOfJedis.getJedis().set("a", "A2", new SetParams());
        mockOfJedis.getJedis().set("b", "B1", new SetParams().nx());
        mockOfJedis.getJedis().set("c", "C1", new SetParams().nx().px(500));
        ScanParams scanParams = new ScanParams();
        scanParams.match("*");
        ScanResult<String> scanResult1 =  mockOfJedis.getJedis().scan("0",scanParams);
        assertEquals(3, scanResult1.getResult().size());
        assertTrue(scanResult1.getResult().contains("a"));
        assertTrue(scanResult1.getResult().contains("b"));
        assertTrue(scanResult1.getResult().contains("c"));
        Thread.sleep(1000);
        assertNull(mockOfJedis.getCurrentData().get("c"));
        ScanResult<String> scanResult2 =  mockOfJedis.getJedis().scan("0",scanParams);
        assertEquals(2, scanResult2.getResult().size());
        assertTrue(scanResult2.getResult().contains("a"));
        assertTrue(scanResult2.getResult().contains("b"));

    }




}
