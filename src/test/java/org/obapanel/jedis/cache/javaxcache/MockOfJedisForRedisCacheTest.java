package org.obapanel.jedis.cache.javaxcache;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.obapanel.jedis.interruptinglocks.JedisLock;
import org.obapanel.jedis.iterators.ScanUtil;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.SetParams;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.obapanel.jedis.cache.javaxcache.MockOfJedisForRedisCache.unitTestEnabledForRedisCache;

@RunWith(MockitoJUnitRunner.class)
public class MockOfJedisForRedisCacheTest {

    private MockOfJedisForRedisCache mockOfJedisForRedisCache;

    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(unitTestEnabledForRedisCache());
        if (!unitTestEnabledForRedisCache()) return;
        mockOfJedisForRedisCache = new MockOfJedisForRedisCache();
    }

    @After
    public void tearDown() {
        if (mockOfJedisForRedisCache != null) {
            mockOfJedisForRedisCache.clearData();
        }
    }

    @Test
    public void testParams() {
        SetParams sp1 = new SetParams();
        boolean t11 = mockOfJedisForRedisCache.isSetParamsNX(sp1);
        boolean t12 = Long.valueOf(1).equals(mockOfJedisForRedisCache.getExpireTimePX(sp1));
        SetParams sp2 = new SetParams();
        sp2.nx();
        boolean t21 = mockOfJedisForRedisCache.isSetParamsNX(sp2);
        boolean t22 = Long.valueOf(1).equals(mockOfJedisForRedisCache.getExpireTimePX(sp2));
        SetParams sp3 = new SetParams();
        sp3.px(1L);
        boolean t31 = mockOfJedisForRedisCache.isSetParamsNX(sp3);
        boolean t32 = Long.valueOf(1).equals(mockOfJedisForRedisCache.getExpireTimePX(sp3));
        SetParams sp4 = new SetParams();
        sp4.nx().px(1L);
        boolean t41 = mockOfJedisForRedisCache.isSetParamsNX(sp4);
        boolean t42 = Long.valueOf(1).equals(mockOfJedisForRedisCache.getExpireTimePX(sp4));

        boolean finalResult = !t11 && !t12 && t21 && !t22 && !t31 && t32 && t41 && t42;
        assertTrue(finalResult);
    }

    @Test
    public void testDataRecovery() throws InterruptedException {
        mockOfJedisForRedisCache.put("a","A1");
        assertEquals("A1", mockOfJedisForRedisCache.getCurrentData().get("a"));
        assertEquals("A1", mockOfJedisForRedisCache.getJedis().get("a"));
        assertTrue(mockOfJedisForRedisCache.getJedis().exists("a"));
        mockOfJedisForRedisCache.put("b","B1");
        assertEquals("B1", mockOfJedisForRedisCache.getCurrentData().get("b"));
        assertEquals("B1", mockOfJedisForRedisCache.getJedis().get("b"));
        assertTrue(mockOfJedisForRedisCache.getJedis().exists("b"));
        mockOfJedisForRedisCache.put("c","C1");
        assertNull(mockOfJedisForRedisCache.getCurrentData().get("d"));
        assertNull(mockOfJedisForRedisCache.getJedis().get("d"));
        assertFalse(mockOfJedisForRedisCache.getJedis().exists("d"));
        assertTrue(mockOfJedisForRedisCache.getJedis().exists("c"));
    }

    @Test
    public void testDataInsertion() throws InterruptedException {
        mockOfJedisForRedisCache.getJedis().set("a", "A1", new SetParams());
        assertEquals("A1", mockOfJedisForRedisCache.getJedis().get("a"));
        assertEquals("A1", mockOfJedisForRedisCache.getCurrentData().get("a"));
        assertTrue(mockOfJedisForRedisCache.getJedis().exists("a"));
        assertTrue(mockOfJedisForRedisCache.getCurrentData().containsKey("a"));
        mockOfJedisForRedisCache.getJedis().set("a", "A2", new SetParams());
        assertEquals("A2", mockOfJedisForRedisCache.getJedis().get("a"));
        assertEquals("A2", mockOfJedisForRedisCache.getCurrentData().get("a"));
        mockOfJedisForRedisCache.getJedis().set("b", "B1", new SetParams().nx());
        assertEquals("B1", mockOfJedisForRedisCache.getJedis().get("b"));
        assertEquals("B1", mockOfJedisForRedisCache.getCurrentData().get("b"));
        mockOfJedisForRedisCache.getJedis().set("b", "B2", new SetParams().nx());
        assertEquals("B1", mockOfJedisForRedisCache.getJedis().get("b"));
        assertEquals("B1", mockOfJedisForRedisCache.getCurrentData().get("b"));
        mockOfJedisForRedisCache.getJedis().set("c", "C1", new SetParams().nx().px(500));
        assertEquals("C1", mockOfJedisForRedisCache.getJedis().get("c"));
        assertEquals("C1", mockOfJedisForRedisCache.getCurrentData().get("c"));
        assertTrue(mockOfJedisForRedisCache.getJedis().exists("c"));
        assertTrue(mockOfJedisForRedisCache.getCurrentData().containsKey("c"));
        mockOfJedisForRedisCache.getJedis().set("d", "D1");
        assertEquals("D1", mockOfJedisForRedisCache.getJedis().get("d"));
        assertEquals("D1", mockOfJedisForRedisCache.getCurrentData().get("d"));
        assertTrue(mockOfJedisForRedisCache.getJedis().exists("d"));
        assertTrue(mockOfJedisForRedisCache.getCurrentData().containsKey("d"));
        mockOfJedisForRedisCache.getJedis().set("c", "C2", new SetParams().nx().px(500));
        assertEquals("C1", mockOfJedisForRedisCache.getJedis().get("c"));
        assertEquals("C1", mockOfJedisForRedisCache.getCurrentData().get("c"));
        Thread.sleep(1000);
        assertNull(mockOfJedisForRedisCache.getCurrentData().get("c"));
        assertFalse(mockOfJedisForRedisCache.getJedis().exists("c"));
        assertFalse(mockOfJedisForRedisCache.getCurrentData().containsKey("c"));
        assertFalse(mockOfJedisForRedisCache.getJedis().exists("j"));
        assertFalse(mockOfJedisForRedisCache.getCurrentData().containsKey("j"));
    }

    @Test
    public void testDataDeletionBasic() throws InterruptedException {
        mockOfJedisForRedisCache.put("a","A1");
        assertEquals("A1", mockOfJedisForRedisCache.getCurrentData().get("a"));
        assertEquals("A1", mockOfJedisForRedisCache.getJedis().get("a"));
        long result1 = mockOfJedisForRedisCache.getJedis().del("a");
        assertTrue(1L == result1);
        assertNull(mockOfJedisForRedisCache.getCurrentData().get("a"));
        assertNull(mockOfJedisForRedisCache.getJedis().get("a"));
        long result2 = mockOfJedisForRedisCache.getJedis().del("b");
        assertTrue(0L == result2);
    }

    @Test
    public void testDataDeletionExtended() throws InterruptedException {
        mockOfJedisForRedisCache.put("a", "A1");
        mockOfJedisForRedisCache.put("b", "B1");
        mockOfJedisForRedisCache.put("c", "C1");
        mockOfJedisForRedisCache.put("d", "D1");
        mockOfJedisForRedisCache.put("e", "E1");
        mockOfJedisForRedisCache.put("f", "F1");
        mockOfJedisForRedisCache.put("g", "G1");
        mockOfJedisForRedisCache.put("h", "H1");
        mockOfJedisForRedisCache.put("i", "I1");
        long delResult1 = mockOfJedisForRedisCache.getJedis().del("b", "e");
        long delResult2 = mockOfJedisForRedisCache.getJedis().del("f");
        long delResult3 = mockOfJedisForRedisCache.getJedis().del("e", "h", "c");
        List<String> expectedKeys = Arrays.asList("a","d","g","i");
        List<String> keys = ScanUtil.retrieveListOfKeys(mockOfJedisForRedisCache.getJedisPool(), "*");
        assertTrue(keys.containsAll(expectedKeys));
        assertTrue(expectedKeys.containsAll(keys));
        assertTrue(2L == delResult1);
        assertTrue(1L == delResult2);
        assertTrue(2L == delResult3);
    }


        @Test
    public void testTransactionInsertionRecovery(){
        mockOfJedisForRedisCache.put("a","A1");
        Transaction t = mockOfJedisForRedisCache.getJedis().multi();
        t.set("b","B1");
        Response<String> response1 = t.get("a");
        try {
            response1.get();
            fail("Should have a JedisDataException by now");
        } catch (Exception e) {
            assertTrue( e instanceof JedisDataException);
        }
        t.exec();
        assertEquals("A1", response1.get());
        assertEquals("B1", mockOfJedisForRedisCache.getJedis().get("b"));
        assertEquals("B1", mockOfJedisForRedisCache.getCurrentData().get("b"));
        Transaction t2 = mockOfJedisForRedisCache.getJedis().multi();
        t2.set("d", "D1", new SetParams());
        t2.exec();
        assertEquals("D1", mockOfJedisForRedisCache.getJedis().get("d"));
        assertEquals("D1", mockOfJedisForRedisCache.getCurrentData().get("d"));
    }

    @Test
    public void testTransactionDeleteRecovery() {
        mockOfJedisForRedisCache.put("a", "A1");
        mockOfJedisForRedisCache.put("b", "B1");
        Transaction t = mockOfJedisForRedisCache.getJedis().multi();
        Response<String> response1 = t.get("a");
        Response<Long> response2 = t.del("a");
        Response<Long> response3 = t.del("b");
        Response<Long> response4 = t.del("c");
        t.exec();
        assertEquals("A1", response1.get());
        assertTrue(1L == response2.get());
        assertTrue(1L == response3.get());
        assertTrue(0L == response4.get());
        assertNull(mockOfJedisForRedisCache.getCurrentData().get("a"));
        assertNull(mockOfJedisForRedisCache.getJedis().get("a"));
        assertNull(mockOfJedisForRedisCache.getCurrentData().get("b"));
        assertNull(mockOfJedisForRedisCache.getJedis().get("b"));
    }

    @Test
    public void testTransactionDeletionExtended() {
        mockOfJedisForRedisCache.put("a", "A1");
        mockOfJedisForRedisCache.put("b", "B1");
        mockOfJedisForRedisCache.put("c", "C1");
        mockOfJedisForRedisCache.put("d", "D1");
        mockOfJedisForRedisCache.put("e", "E1");
        mockOfJedisForRedisCache.put("f", "F1");
        mockOfJedisForRedisCache.put("g", "G1");
        mockOfJedisForRedisCache.put("h", "H1");
        mockOfJedisForRedisCache.put("i", "I1");
        Transaction transaction = mockOfJedisForRedisCache.getJedis().multi();
        Response<Long> rdelResult1 = transaction.del("b", "e");
        Response<Long> rdelResult2 = transaction.del("f");
        Response<Long> rdelResult3 = transaction.del("e", "h", "c");
        transaction.exec();
        long delResult1 = rdelResult1.get();
        long delResult2 = rdelResult2.get();
        long delResult3 = rdelResult3.get();
        List<String> expectedKeys = Arrays.asList("a","d","g","i");
        List<String> keys = ScanUtil.retrieveListOfKeys(mockOfJedisForRedisCache.getJedisPool(), "*");
        assertTrue(keys.containsAll(expectedKeys));
        assertTrue(expectedKeys.containsAll(keys));
        assertTrue(2L == delResult1);
        assertTrue(1L == delResult2);
        assertTrue(2L == delResult3);
    }

    @Test
    public void testScan() {
        mockOfJedisForRedisCache.put("a", "A1");
        mockOfJedisForRedisCache.put("b", "B1");
        mockOfJedisForRedisCache.put("c", "C1");
        mockOfJedisForRedisCache.put("e", "E1");
        List<String> expectedKeys1 = Arrays.asList("e","b","c","a");
        List<String> keys1 = ScanUtil.retrieveListOfKeys(mockOfJedisForRedisCache.getJedisPool(), "*");
        assertTrue(keys1.containsAll(expectedKeys1));
        assertTrue(expectedKeys1.containsAll(keys1));
        long delResult = mockOfJedisForRedisCache.getJedis().del("b", "e");
        List<String> expectedKeys2 = Arrays.asList("a","c");
        List<String> keys2 = ScanUtil.retrieveListOfKeys(mockOfJedisForRedisCache.getJedisPool(), "*");
        assertTrue(keys2.containsAll(expectedKeys2));
        assertTrue(expectedKeys2.containsAll(keys2));
        assertEquals(2L, delResult);
    }



        @Test
    public void testEval() {
        List<String> keys = Collections.singletonList("a");
        List<String> values = Collections.singletonList("A1");
        Object response = mockOfJedisForRedisCache.getJedis().eval(JedisLock.UNLOCK_LUA_SCRIPT, keys, values);
        assertNull( response);
    }

}
