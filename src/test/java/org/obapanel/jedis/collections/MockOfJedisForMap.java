package org.obapanel.jedis.collections;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Builder;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.obapanel.jedis.common.test.TransactionOrder.quickReponseExecuted;

public class MockOfJedisForMap {

    private static final Logger LOG = LoggerFactory.getLogger(MockOfJedisForMap.class);

    public static final String CLIENT_RESPONSE_OK = "OK";
    public static final String CLIENT_RESPONSE_KO = "KO";


    // Zero to prevent some unit test
    // One to one pass
    // More to more passes
    static final int UNIT_TEST_CYCLES_LIST = 1;

    static boolean unitTestEnabledForList(){
        return UNIT_TEST_CYCLES_LIST > 0;
    }

    private final Jedis jedis;
    private final JedisPool jedisPool;
    private final Transaction transaction;
    private final Map<String, Object> data = Collections.synchronizedMap(new HashMap<>());
    private final Timer timer;

    public MockOfJedisForMap() {
        timer = new Timer();

        jedis = Mockito.mock(Jedis.class);
        jedisPool = Mockito.mock(JedisPool.class);
        when(jedisPool.getResource()).thenReturn(jedis);

        transaction = Mockito.mock(Transaction.class);

        when(jedis.multi()).thenReturn(transaction);
        when(jedis.exists(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockExists(key);
        });
        when(jedis.hget(anyString(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String name = ioc.getArgument(1);
            return mockHget(key, name);
        });
        when(jedis.hset(anyString(), anyString(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String name = ioc.getArgument(1);
            String value = ioc.getArgument(2);
            return mockHset(key, name, value);
        });
        when(transaction.hget(anyString(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String name = ioc.getArgument(1);
            return mockTransactionHget(key, name);
        });
        when(transaction.hset(anyString(), anyString(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String name = ioc.getArgument(1);
            String value = ioc.getArgument(2);
            return mockTransactionHset(key, name, value);
        });
    }



    Jedis getJedis(){
        return jedis;
    }

    JedisPool getJedisPool() {
        return jedisPool;
    }

    synchronized void clearData(){
        data.clear();
    }


    synchronized Map<String,Object> getCurrentData() {
        return new HashMap<>(data);
    }

    synchronized void put(String key, Object element) {
        data.put(key, element);
    }

    synchronized boolean mockExists(String key) {
        return data.containsKey(key);
    }

    synchronized String mockHget(String key, String name) {
        Map<String, String> map = (Map<String, String>) data.get(key);
        return map != null ? map.get(name) : null;
    }

    synchronized Response<String> mockTransactionHget(String key, String name) {
        String data = mockHget(key, name);
        return quickReponseExecuted(data);
    }


    synchronized Long mockHset(String key, String name, String value) {
        Map<String, String> map = (Map<String, String>) data.computeIfAbsent(key, k->new HashMap<String, String>());
        map.put(name, value);
        return 1L;
    }

    synchronized Response<Long> mockTransactionHset(String key, String name, String value) {
        Long data = mockHset(key, name, value);
        return quickReponseExecuted(data);
    }


}
