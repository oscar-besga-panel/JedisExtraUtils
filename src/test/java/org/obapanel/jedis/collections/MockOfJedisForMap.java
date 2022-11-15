package org.obapanel.jedis.collections;

import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBase;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.obapanel.jedis.common.test.TransactionOrder.quickReponseExecuted;

public class MockOfJedisForMap {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockOfJedisForMap.class);

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

    private final Map<String, Object> data = Collections.synchronizedMap(new HashMap<>());
    private final Timer timer;

    public MockOfJedisForMap() {
        PowerMockito.suppress(MemberMatcher.methodsDeclaredIn(TransactionBase.class));

        timer = new Timer();

        jedis = Mockito.mock(Jedis.class);
        jedisPool = Mockito.mock(JedisPool.class);
        when(jedisPool.getResource()).thenReturn(jedis);

        Transaction transaction = PowerMockito.mock(Transaction.class);

        when(jedis.multi()).thenReturn(transaction);
        when(jedis.exists(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockExists(key);
        });
        when(jedis.del(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockDelete(key);
        });
        when(jedis.hlen(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockHlen(key);
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
        when(jedis.hdel(anyString(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String name = ioc.getArgument(1);
            return mockHdel(key, name);
        });
        when(jedis.hexists(anyString(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String name = ioc.getArgument(1);
            return mockHexists(key, name);
        });
        when(jedis.hscan(anyString(), anyString(), any(ScanParams.class))).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String cursor = ioc.getArgument(1);
            ScanParams scanParams = ioc.getArgument(2);
            return mockHscan(key, cursor, scanParams);
        });
        when(jedis.hscan(anyString(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String cursor = ioc.getArgument(1);
            ScanParams scanParams = new ScanParams();
            return mockHscan(key, cursor, scanParams);
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
        when(transaction.hdel(anyString(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String name = ioc.getArgument(1);
            return mockTransactionHdel(key, name);
        });
        PowerMockito.when(transaction.exec()).thenAnswer(ioc -> mockTransactionExec());
    }



    Jedis getJedis(){
        return jedis;
    }

    JedisPool getJedisPool() {
        return jedisPool;
    }

    private synchronized Map<String, String> getStringStringMap(String key) {
        return (Map<String, String>) data.computeIfAbsent(key, k -> new HashMap<String, String>());
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

    synchronized Long mockDelete(String key) {
        Object previous =  data.remove(key);
        return previous != null ? 1L : 0L;
    }

    synchronized Long mockHlen(String key) {
        Map<String, String> map = getStringStringMap(key);
        return map != null ? map.size() : 0L;
    }

    synchronized Boolean mockHexists(String key, String name) {
        Map<String, String> map = getStringStringMap(key);
        return map != null && map.containsKey(name);
    }

    synchronized String mockHget(String key, String name) {
        Map<String, String> map = getStringStringMap(key);
        return map != null ? map.get(name) : null;
    }

    synchronized Response<String> mockTransactionHget(String key, String name) {
        String data = mockHget(key, name);
        return quickReponseExecuted(data);
    }


    synchronized Long mockHset(String key, String name, String value) {
        Map<String, String> map = getStringStringMap(key);
        map.put(name, value);
        return 1L;
    }

    synchronized Response<Long> mockTransactionHset(String key, String name, String value) {
        Long data = mockHset(key, name, value);
        return quickReponseExecuted(data);
    }

    synchronized Long mockHdel(String key, String name) {
        Map<String, String> map = getStringStringMap(key);
        if (map != null) {
            String previous = map.remove(name);
            return previous != null ? 1L : 0L;
        } else {
            return 0L;
        }
    }

    synchronized Response<Long> mockTransactionHdel(String key, String name) {
        Long data = mockHdel(key, name);
        return quickReponseExecuted(data);
    }

    synchronized ScanResult<Map.Entry<String, String>> mockHscan(String key, String cursor, ScanParams scanParams) {
        Map<String, String> map = getStringStringMap(key);
        List<Map.Entry<String, String>> results = new ArrayList<>(map.entrySet());
        return new ScanResult<Map.Entry<String, String>>(ScanParams.SCAN_POINTER_START, results);
    }

    private Object mockTransactionExec() {
        LOGGER.debug("mockTransactionExec do nothing");
        return new ArrayList<Object>(0);
    }

}
