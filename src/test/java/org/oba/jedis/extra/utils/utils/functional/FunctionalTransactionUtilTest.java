package org.oba.jedis.extra.utils.utils.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.oba.jedis.extra.utils.iterators.ScanUtil.deleteListOfKeysStartsWith;
import static org.oba.jedis.extra.utils.utils.TransactionUtil.withinMultiDo;
import static org.oba.jedis.extra.utils.utils.TransactionUtil.withinMultiGet;

public class FunctionalTransactionUtilTest {

    private static final String COMMON_REDIS_TEST_NAME = "util:" + FunctionalTransactionUtilTest.class.getName() + ":";

    private static final List<String> listNameKeysToDelete = new ArrayList<>();

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private RedisClient redisClient;

    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        redisClient = jtfTest.createRedisClient();
    }

    @After
    public void tearDown() {
        if (redisClient != null) {
            listNameKeysToDelete.forEach( k -> redisClient.del(k));
            deleteListOfKeysStartsWith(redisClient, COMMON_REDIS_TEST_NAME);
            redisClient.close();
        }
    }

    String createName() {
        String newName = COMMON_REDIS_TEST_NAME + System.currentTimeMillis();
        listNameKeysToDelete.add(newName);
        return newName;
    }

    @Test
    public void withinMultiDoTest() {
        final String key = createName();
        final String value = Long.toHexString(System.nanoTime());
        AtomicReference<String> result = new AtomicReference<>("");
        withinMultiDo(redisClient, trs -> {
            trs.set(key, value);
            Response<String> responseGet = trs.get(key);
            trs.exec();
            result.set(responseGet.get());
        });
        assertEquals(value, result.get());
        assertEquals(value, redisClient.get(key));
    }

    @Test
    public void withinMultiGetTest() {
        final String key = createName();
        final String value = Long.toHexString(System.nanoTime());
        String result = withinMultiGet(redisClient, trs -> {
            trs.set(key, value);
            Response<String> responseGet = trs.get(key);
            trs.exec();
            return responseGet.get();
        });
        assertEquals(value, result);
        assertEquals(value, redisClient.get(key));
    }

}
