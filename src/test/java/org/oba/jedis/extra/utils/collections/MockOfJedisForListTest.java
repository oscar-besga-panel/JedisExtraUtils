package org.oba.jedis.extra.utils.collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBase;
import redis.clients.jedis.args.ListPosition;
import redis.clients.jedis.params.SetParams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.oba.jedis.extra.utils.collections.MockOfJedisForList.CLIENT_RESPONSE_KO;
import static org.oba.jedis.extra.utils.collections.MockOfJedisForList.CLIENT_RESPONSE_OK;


@RunWith(PowerMockRunner.class)
@PrepareForTest({Transaction.class, TransactionBase.class })
public class MockOfJedisForListTest {


    private MockOfJedisForList mockOfJedis;

    @Before
    public void setup() {
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

    @Test
    public void testMockExists() {
        mockOfJedis.put("data1","data1value");
        assertTrue(mockOfJedis.mockExists("data1"));
        assertFalse(mockOfJedis.mockExists("data2"));
    }

    @Test
    public void testMockGet() {
        mockOfJedis.put("data1","data1value");
        assertEquals("data1value", mockOfJedis.mockGet("data1"));
        assertNull(mockOfJedis.mockGet("data2"));
    }

    @Test
    public void testMockSet() {
        String result11 = mockOfJedis.mockSet("data1","data1value1", new SetParams());
        assertEquals("data1value1", mockOfJedis.mockGet("data1"));
        assertEquals(CLIENT_RESPONSE_OK, result11);
        String result12 = mockOfJedis.mockSet("data1","data1value2", new SetParams());
        assertEquals("data1value2", mockOfJedis.mockGet("data1"));
        assertEquals(CLIENT_RESPONSE_OK, result12);
        String result13 = mockOfJedis.mockSet("data1","data1value3", new SetParams().nx());
        assertEquals("data1value2", mockOfJedis.mockGet("data1"));
        assertEquals(CLIENT_RESPONSE_KO, result13);
        String result14 = mockOfJedis.mockSet("data2","data1value3", new SetParams().nx());
        assertEquals("data1value3", mockOfJedis.mockGet("data2"));
        assertEquals(CLIENT_RESPONSE_OK, result14);
        assertNull(mockOfJedis.mockGet("data3"));
    }

    @Test
    public void testMockDel() {
        mockOfJedis.put("data1","data1value");
        assertEquals("data1value", mockOfJedis.mockGet("data1"));
        mockOfJedis.mockDel("data1");
        assertNull(mockOfJedis.mockGet("data1"));
    }


    @Test
    public void testMockListLrange () {
        mockOfJedis.put("data1", new ArrayList<>(Arrays.asList("a","b","c","d","e")));
        List<String> result1 = mockOfJedis.mockListLrange("data1", 1,3);
        List<String> result2 = mockOfJedis.mockListLrange("data1", 0,-1);
        assertEquals(new ArrayList<>(Arrays.asList("b","c","d")), result1);
        assertEquals(new ArrayList<>(Arrays.asList("a","b","c","d","e")), result2);
    }

    @Test
    public void testMockListRpush () {
        mockOfJedis.put("data1", new ArrayList<>(Arrays.asList("a","b","c")));
        mockOfJedis.mockListRpush("data1",new String[]{"d","e"});
        assertEquals(new ArrayList<>(Arrays.asList("a","b","c","d","e")), mockOfJedis.mockGet("data1"));
    }

    @Test
    public void testMockListIndex () {
        mockOfJedis.put("data1", new ArrayList<>(Arrays.asList("a","b","c")));
        String a = mockOfJedis.mockListLindex("data1",0);
        String c = mockOfJedis.mockListLindex("data1",2);
        assertEquals("a",a);
        assertEquals("c",c);
    }

    @Test
    public void testMockListLlrem() {
        mockOfJedis.put("data1", new ArrayList<>(Arrays.asList("a","b","c","d","a")));
        long result1 = mockOfJedis.mockListLlrem("data1",2, "a");
        mockOfJedis.put("data2", new ArrayList<>(Arrays.asList("a","b","c","d","a")));
        long result2 = mockOfJedis.mockListLlrem("data2",1, "a");
        assertEquals(new ArrayList<>(Arrays.asList("b","c","d")), mockOfJedis.mockGet("data1"));
        assertEquals(2L, result1);
        assertEquals(new ArrayList<>(Arrays.asList("b","c","d","a")), mockOfJedis.mockGet("data2"));
        assertEquals(1L, result2);
    }

    @Test
    public void testMockListLset() {
        mockOfJedis.put("data1", new ArrayList<>(Arrays.asList("a","b","c","d")));
        mockOfJedis.mockListLset("data1",2,"x");
        assertEquals(new ArrayList<>(Arrays.asList("a","b","x","d")), mockOfJedis.mockGet("data1"));
    }

    @Test
    public void testMockEval() {
        String sha1IndexOf = mockOfJedis.mockScriptLoad("mock indexOf");
        String sha1LastIndexOf = mockOfJedis.mockScriptLoad("mock lastIndexOf");
        mockOfJedis.put("data1", new ArrayList<>(Arrays.asList("a","b","c","d","b")));
        Object result1 = mockOfJedis.mockEvalSha(sha1IndexOf, Collections.singletonList("data1"), Collections.singletonList("b"));
        Object result2 = mockOfJedis.mockEvalSha(sha1LastIndexOf, Collections.singletonList("data1"), Collections.singletonList("b"));
        assertEquals(Long.valueOf(1), (Long) result1);
        assertEquals(Long.valueOf(4), (Long) result2);
    }

    @Test
    public void testMockListLlinsert() {
        mockOfJedis.put("data1", new ArrayList<>(Arrays.asList("a","b","c","d")));
        mockOfJedis.mockListLlinsert("data1", ListPosition.BEFORE, "b", "x");
        mockOfJedis.mockListLlinsert("data1", ListPosition.AFTER, "c", "y");
        assertEquals(new ArrayList<>(Arrays.asList("a","x","b","c","y","d")), mockOfJedis.mockGet("data1"));
    }
}
