package org.oba.jedis.extra.utils.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.cache.functional.FunctionalSimpleCacheIteratorTest;
import redis.clients.jedis.Response;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.oba.jedis.extra.utils.utils.MockOfJedis.unitTestEnabled;
import static org.oba.jedis.extra.utils.utils.TransactionUtil.withinMultiDo;
import static org.oba.jedis.extra.utils.utils.TransactionUtil.withinMultiGet;

public class TransactionUtilTest {

    private static final String COMMON_REDIS_TEST_NAME = "util:" + TransactionUtilTest.class.getName() + ":";


    private MockOfJedis mockOfJedis;

    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(unitTestEnabled());
        if (!unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
    }

    @After
    public void after() throws IOException {
        mockOfJedis.getRedisClient().close();
        mockOfJedis.clearData();
    }

    String createName() {
        return COMMON_REDIS_TEST_NAME + System.currentTimeMillis();
    }

    @Test
    public void withinMultiDoTest() {
        final String key = createName();
        final String value = Long.toHexString(System.nanoTime());
        AtomicReference<String> result = new AtomicReference<>("");
        withinMultiDo(mockOfJedis.getRedisClient(), trs -> {
            trs.set(key, value);
            Response<String> responseGet = trs.get(key);
            trs.exec();
            result.set(responseGet.get());
        });
        verify( mockOfJedis.getRedisClient(), times(1)).multi();
        verify( mockOfJedis.getTransaction(), times(1)).set(anyString(), anyString());
        verify( mockOfJedis.getTransaction(), times(1)).exec();
        verify( mockOfJedis.getTransaction(), times(1)).close();
        assertEquals(value, result.get());
        assertEquals(mockOfJedis.getCurrentTransactionData().get(key), result.get());
    }

    @Test
    public void withinMultiGetTest() {
        final String key = createName();
        final String value = Long.toHexString(System.nanoTime());
        String result = withinMultiGet(mockOfJedis.getRedisClient(), trs -> {
            trs.set(key, value);
            Response<String> responseGet = trs.get(key);
            trs.exec();
            return responseGet.get();
        });
        verify( mockOfJedis.getRedisClient(), times(1)).multi();
        verify( mockOfJedis.getTransaction(), times(1)).set(anyString(), anyString());
        verify( mockOfJedis.getTransaction(), times(1)).exec();
        verify( mockOfJedis.getTransaction(), times(1)).close();
        assertEquals(value, result);
        assertEquals(mockOfJedis.getCurrentTransactionData().get(key), result);
    }




}
