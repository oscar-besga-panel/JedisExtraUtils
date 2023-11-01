package org.oba.jedis.extra.utils.countdownlatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.params.SetParams;

import static org.junit.Assert.*;

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
    public void testDataGetSetDecrDel() throws InterruptedException {
        mockOfJedis.getJedis().set("a", "5", new SetParams().nx());
        assertEquals("5", mockOfJedis.getCurrentData().get("a"));
        mockOfJedis.getJedis().set("a", "7", new SetParams().nx());
        assertEquals("5", mockOfJedis.getCurrentData().get("a"));
        assertEquals("5", mockOfJedis.getJedis().get("a"));
        assertTrue(4L == mockOfJedis.getJedis().decr("a"));
        assertEquals("4", mockOfJedis.getJedis().get("a"));
        assertTrue(1L == mockOfJedis.getJedis().del("a"));
        assertNull(mockOfJedis.getCurrentData().get("a"));
    }


}
