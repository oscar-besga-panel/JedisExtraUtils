package org.obapanel.jedis.iterators;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.semaphore.JedisSemaphore;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.params.SetParams;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.obapanel.jedis.iterators.MockOfJedis.unitTestEnabled;

public class MockOfJedisTest {


    private MockOfJedis mockOfJedis;

    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(unitTestEnabled());
        if (!unitTestEnabled()) return;
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
        assertEquals(Long.valueOf(1), mockOfJedis.getJedis().del("a"));
        assertNull(mockOfJedis.getCurrentData().get("a"));
        assertEquals(Long.valueOf(0), mockOfJedis.getJedis().del("a"));
    }

    @Test
    public void testMockScan() {
        mockOfJedis.getJedis().set("a", "A1", new SetParams());
        mockOfJedis.getJedis().set("b", "B1", new SetParams());
        mockOfJedis.getJedis().set("c", "C1", new SetParams());
        ScanResult<String> result = mockOfJedis.getJedis().scan("");
        assertEquals(ScanParams.SCAN_POINTER_START, result.getCursor());
        assertTrue( result.getResult().contains("a"));
        assertTrue( result.getResult().contains("b"));
        assertTrue( result.getResult().contains("c"));
    }


}

