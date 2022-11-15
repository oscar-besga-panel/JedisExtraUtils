package org.obapanel.jedis.cache.simple;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.obapanel.jedis.interruptinglocks.JedisLock;
import org.obapanel.jedis.iterators.ScanUtil;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBase;
import redis.clients.jedis.params.SetParams;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.obapanel.jedis.cache.simple.MockOfJedisForSimpleCache.unitTestEnabledForSimpleCache;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Transaction.class, TransactionBase.class })
public class MockOfJedisForSimpleCacheTest {

    private MockOfJedisForSimpleCache mockOfJedisForSimpleCache;

    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(unitTestEnabledForSimpleCache());
        if (!unitTestEnabledForSimpleCache()) return;
        mockOfJedisForSimpleCache = new MockOfJedisForSimpleCache();
    }

    @After
    public void tearDown() {
        if (mockOfJedisForSimpleCache != null) {
            mockOfJedisForSimpleCache.clearData();
        }
    }

    @Test
    public void testParams() {
        SetParams sp1 = new SetParams();
        boolean t11 = mockOfJedisForSimpleCache.isSetParamsNX(sp1);
        boolean t12 = Long.valueOf(1).equals(mockOfJedisForSimpleCache.getExpireTimePX(sp1));
        SetParams sp2 = new SetParams();
        sp2.nx();
        boolean t21 = mockOfJedisForSimpleCache.isSetParamsNX(sp2);
        boolean t22 = Long.valueOf(1).equals(mockOfJedisForSimpleCache.getExpireTimePX(sp2));
        SetParams sp3 = new SetParams();
        sp3.px(1L);
        boolean t31 = mockOfJedisForSimpleCache.isSetParamsNX(sp3);
        boolean t32 = Long.valueOf(1).equals(mockOfJedisForSimpleCache.getExpireTimePX(sp3));
        SetParams sp4 = new SetParams();
        sp4.nx().px(1L);
        boolean t41 = mockOfJedisForSimpleCache.isSetParamsNX(sp4);
        boolean t42 = Long.valueOf(1).equals(mockOfJedisForSimpleCache.getExpireTimePX(sp4));

        boolean finalResult = !t11 && !t12 && t21 && !t22 && !t31 && t32 && t41 && t42;
        assertTrue(finalResult);
    }

    @Test
    public void testDataRecovery() {
        mockOfJedisForSimpleCache.put("a","A1");
        assertEquals("A1", mockOfJedisForSimpleCache.getCurrentData().get("a"));
        assertEquals("A1", mockOfJedisForSimpleCache.getJedis().get("a"));
        assertTrue(mockOfJedisForSimpleCache.getJedis().exists("a"));
        mockOfJedisForSimpleCache.put("b","B1");
        assertEquals("B1", mockOfJedisForSimpleCache.getCurrentData().get("b"));
        assertEquals("B1", mockOfJedisForSimpleCache.getJedis().get("b"));
        assertTrue(mockOfJedisForSimpleCache.getJedis().exists("b"));
        mockOfJedisForSimpleCache.put("c","C1");
        assertNull(mockOfJedisForSimpleCache.getCurrentData().get("d"));
        assertNull(mockOfJedisForSimpleCache.getJedis().get("d"));
        assertFalse(mockOfJedisForSimpleCache.getJedis().exists("d"));
        assertTrue(mockOfJedisForSimpleCache.getJedis().exists("c"));
    }

    @Test
    public void testDataInsertion() throws InterruptedException {
        mockOfJedisForSimpleCache.getJedis().set("a", "A1", new SetParams());
        assertEquals("A1", mockOfJedisForSimpleCache.getJedis().get("a"));
        assertEquals("A1", mockOfJedisForSimpleCache.getCurrentData().get("a"));
        assertTrue(mockOfJedisForSimpleCache.getJedis().exists("a"));
        assertTrue(mockOfJedisForSimpleCache.getCurrentData().containsKey("a"));
        mockOfJedisForSimpleCache.getJedis().set("a", "A2", new SetParams());
        assertEquals("A2", mockOfJedisForSimpleCache.getJedis().get("a"));
        assertEquals("A2", mockOfJedisForSimpleCache.getCurrentData().get("a"));
        mockOfJedisForSimpleCache.getJedis().set("b", "B1", new SetParams().nx());
        assertEquals("B1", mockOfJedisForSimpleCache.getJedis().get("b"));
        assertEquals("B1", mockOfJedisForSimpleCache.getCurrentData().get("b"));
        mockOfJedisForSimpleCache.getJedis().set("b", "B2", new SetParams().nx());
        assertEquals("B1", mockOfJedisForSimpleCache.getJedis().get("b"));
        assertEquals("B1", mockOfJedisForSimpleCache.getCurrentData().get("b"));
        mockOfJedisForSimpleCache.getJedis().set("c", "C1", new SetParams().nx().px(500));
        assertEquals("C1", mockOfJedisForSimpleCache.getJedis().get("c"));
        assertEquals("C1", mockOfJedisForSimpleCache.getCurrentData().get("c"));
        assertTrue(mockOfJedisForSimpleCache.getJedis().exists("c"));
        assertTrue(mockOfJedisForSimpleCache.getCurrentData().containsKey("c"));
        mockOfJedisForSimpleCache.getJedis().set("d", "D1");
        assertEquals("D1", mockOfJedisForSimpleCache.getJedis().get("d"));
        assertEquals("D1", mockOfJedisForSimpleCache.getCurrentData().get("d"));
        assertTrue(mockOfJedisForSimpleCache.getJedis().exists("d"));
        assertTrue(mockOfJedisForSimpleCache.getCurrentData().containsKey("d"));
        mockOfJedisForSimpleCache.getJedis().set("c", "C2", new SetParams().nx().px(500));
        assertEquals("C1", mockOfJedisForSimpleCache.getJedis().get("c"));
        assertEquals("C1", mockOfJedisForSimpleCache.getCurrentData().get("c"));
        Thread.sleep(1000);
        assertNull(mockOfJedisForSimpleCache.getCurrentData().get("c"));
        assertFalse(mockOfJedisForSimpleCache.getJedis().exists("c"));
        assertFalse(mockOfJedisForSimpleCache.getCurrentData().containsKey("c"));
        assertFalse(mockOfJedisForSimpleCache.getJedis().exists("j"));
        assertFalse(mockOfJedisForSimpleCache.getCurrentData().containsKey("j"));
    }

    @Test
    public void testDataDeletionBasic() throws InterruptedException {
        mockOfJedisForSimpleCache.put("a","A1");
        assertEquals("A1", mockOfJedisForSimpleCache.getCurrentData().get("a"));
        assertEquals("A1", mockOfJedisForSimpleCache.getJedis().get("a"));
        long result1 = mockOfJedisForSimpleCache.getJedis().del("a");
        assertEquals(1L, result1);
        assertNull(mockOfJedisForSimpleCache.getCurrentData().get("a"));
        assertNull(mockOfJedisForSimpleCache.getJedis().get("a"));
        long result2 = mockOfJedisForSimpleCache.getJedis().del("b");
        assertEquals(0L, result2);
    }

    @Test
    public void testDataDeletionExtended() throws InterruptedException {
        mockOfJedisForSimpleCache.put("a", "A1");
        mockOfJedisForSimpleCache.put("b", "B1");
        mockOfJedisForSimpleCache.put("c", "C1");
        mockOfJedisForSimpleCache.put("d", "D1");
        mockOfJedisForSimpleCache.put("e", "E1");
        mockOfJedisForSimpleCache.put("f", "F1");
        mockOfJedisForSimpleCache.put("g", "G1");
        mockOfJedisForSimpleCache.put("h", "H1");
        mockOfJedisForSimpleCache.put("i", "I1");
        long delResult1 = mockOfJedisForSimpleCache.getJedis().del("b", "e");
        long delResult2 = mockOfJedisForSimpleCache.getJedis().del("f");
        long delResult3 = mockOfJedisForSimpleCache.getJedis().del("e", "h", "c");
        List<String> expectedKeys = Arrays.asList("a","d","g","i");
        List<String> keys = ScanUtil.retrieveListOfKeys(mockOfJedisForSimpleCache.getJedisPool(), "*");
        assertTrue(keys.containsAll(expectedKeys));
        assertTrue(expectedKeys.containsAll(keys));
        assertEquals(2L, delResult1);
        assertEquals(1L, delResult2);
        assertEquals(2L, delResult3);
    }


    @Test
    public void testTransactionInsertionRecovery(){
        mockOfJedisForSimpleCache.put("a","A1");
        Transaction t = mockOfJedisForSimpleCache.getJedis().multi();
        t.set("b","B1");
        Response<String> response1 = t.get("a");
        try {
            response1.get();
            fail("Should have a JedisDataException by now");
        } catch (IllegalStateException e) {
            assertNotNull(e.getMessage());
        } catch (Exception e) {
            fail("Should have a JedisDataException by now");
        }
        t.exec();
        assertEquals("A1", response1.get());
        assertEquals("B1", mockOfJedisForSimpleCache.getJedis().get("b"));
        assertEquals("B1", mockOfJedisForSimpleCache.getCurrentData().get("b"));
        Transaction t2 = mockOfJedisForSimpleCache.getJedis().multi();
        t2.set("d", "D1", new SetParams());
        t2.exec();
        assertEquals("D1", mockOfJedisForSimpleCache.getJedis().get("d"));
        assertEquals("D1", mockOfJedisForSimpleCache.getCurrentData().get("d"));
    }

    @Test
    public void testTransactionDeleteRecovery() {
        mockOfJedisForSimpleCache.put("a", "A1");
        mockOfJedisForSimpleCache.put("b", "B1");
        Transaction t = mockOfJedisForSimpleCache.getJedis().multi();
        Response<String> response1 = t.get("a");
        Response<Long> response2 = t.del("a");
        Response<Long> response3 = t.del("b");
        Response<Long> response4 = t.del("c");
        t.exec();
        assertEquals("A1", response1.get());
        assertEquals(1L, (long) response2.get());
        assertEquals(1L, (long) response3.get());
        assertEquals(0L, (long) response4.get());
        assertNull(mockOfJedisForSimpleCache.getCurrentData().get("a"));
        assertNull(mockOfJedisForSimpleCache.getJedis().get("a"));
        assertNull(mockOfJedisForSimpleCache.getCurrentData().get("b"));
        assertNull(mockOfJedisForSimpleCache.getJedis().get("b"));
    }

    @Test
    public void testTransactionDeletionExtended() {
        mockOfJedisForSimpleCache.put("a", "A1");
        mockOfJedisForSimpleCache.put("b", "B1");
        mockOfJedisForSimpleCache.put("c", "C1");
        mockOfJedisForSimpleCache.put("d", "D1");
        mockOfJedisForSimpleCache.put("e", "E1");
        mockOfJedisForSimpleCache.put("f", "F1");
        mockOfJedisForSimpleCache.put("g", "G1");
        mockOfJedisForSimpleCache.put("h", "H1");
        mockOfJedisForSimpleCache.put("i", "I1");
        Transaction transaction = mockOfJedisForSimpleCache.getJedis().multi();
        Response<Long> rdelResult1 = transaction.del("b", "e");
        Response<Long> rdelResult2 = transaction.del("f");
        Response<Long> rdelResult3 = transaction.del("e", "h", "c");
        transaction.exec();
        long delResult1 = rdelResult1.get();
        long delResult2 = rdelResult2.get();
        long delResult3 = rdelResult3.get();
        List<String> expectedKeys = Arrays.asList("a","d","g","i");
        List<String> keys = ScanUtil.retrieveListOfKeys(mockOfJedisForSimpleCache.getJedisPool(), "*");
        assertTrue(keys.containsAll(expectedKeys));
        assertTrue(expectedKeys.containsAll(keys));
        assertEquals(2L, delResult1);
        assertEquals(1L, delResult2);
        assertEquals(2L, delResult3);
    }

    @Test
    public void testScan() {
        mockOfJedisForSimpleCache.put("a", "A1");
        mockOfJedisForSimpleCache.put("b", "B1");
        mockOfJedisForSimpleCache.put("c", "C1");
        mockOfJedisForSimpleCache.put("e", "E1");
        List<String> expectedKeys1 = Arrays.asList("e","b","c","a");
        List<String> keys1 = ScanUtil.retrieveListOfKeys(mockOfJedisForSimpleCache.getJedisPool(), "*");
        assertTrue(keys1.containsAll(expectedKeys1));
        assertTrue(expectedKeys1.containsAll(keys1));
        long delResult = mockOfJedisForSimpleCache.getJedis().del("b", "e");
        List<String> expectedKeys2 = Arrays.asList("a","c");
        List<String> keys2 = ScanUtil.retrieveListOfKeys(mockOfJedisForSimpleCache.getJedisPool(), "*");
        assertTrue(keys2.containsAll(expectedKeys2));
        assertTrue(expectedKeys2.containsAll(keys2));
        assertEquals(2L, delResult);
    }



        @Test
    public void testEval() {
        List<String> keys = Collections.singletonList("a");
        List<String> values = Collections.singletonList("A1");
        Object response = mockOfJedisForSimpleCache.getJedis().eval(JedisLock.UNLOCK_LUA_SCRIPT, keys, values);
        assertNull( response);
    }

}
