package org.obapanel.jedis.collections;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Transaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Mock of jedis methods used by the lock
 */
public class MockOfJedisForSet {

    private static final Logger LOG = LoggerFactory.getLogger(MockOfJedisForList.class);

    public static final String CLIENT_RESPONSE_OK = "OK";
    public static final String CLIENT_RESPONSE_KO = "KO";


    // Zero to prevent some unit test
    // One to one pass
    // More to more passes
    static final int UNIT_TEST_CYCLES_LIST = 1;

    static boolean unitTestEnabledForList() {
        return UNIT_TEST_CYCLES_LIST > 0;
    }

    private final Jedis jedis;
    private final JedisPool jedisPool;
    private final Transaction transaction;
    private final Map<String, Object> data = Collections.synchronizedMap(new HashMap<>());
    private final Timer timer;

    public MockOfJedisForSet() {
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
        when(jedis.del(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockDel(key);
        });
        when(jedis.sadd(anyString(), any())).thenAnswer(ioc -> {
            // dont like it, but it works
            String key = ioc.getArgument(0);
            Object value;
            if (ioc.getArguments().length > 2) {
                // String[] has beem passed as many arguments
                List<String> temp = new ArrayList<>();
                for(int i=1; i < ioc.getArguments().length; i++) {
                    temp.add(ioc.getArgument(i));
                }
                value = temp.toArray(new String[0]);
            } else {
                // passed String[]
                value = ioc.getArgument(1);
            }
            return mockSadd(key, value);
        });


        when(jedis.sismember(anyString(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            return mockSismember(key, value);
        });
        when(jedis.scard(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockScard(key);
        });

        when(jedis.sscan(anyString(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String cursor = ioc.getArgument(1);
            ScanParams scanParams = new ScanParams();
            return mockSscan(key, cursor, scanParams);
        });
        when(jedis.sscan(anyString(), anyString(), any(ScanParams.class))).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String cursor = ioc.getArgument(1);
            ScanParams scanParams = ioc.getArgument(2);
            return mockSscan(key, cursor, scanParams);
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


    private synchronized Set<String> getStringSet(String key) {
        return (Set<String>) data.computeIfAbsent(key, k -> new HashSet<String>());
    }

    synchronized boolean mockExists(String key) {
        return data.containsKey(key);
    }

    synchronized Long mockDel(String key) {
        if (data.containsKey(key)) {
            data.remove(key);
            return 1L;
        } else {
            return 0L;
        }
    }

    synchronized Long mockSadd(String key, Object value) {
        if (value instanceof String) {
            return doAdd(key, new String[] {(String) value});
        } else if (value instanceof String[]) {
            return doAdd(key, (String[]) value);
        } else {
            return 0L;
        }
    }

    private Long doAdd(String key, String[] values) {
        Set<String> set = getStringSet(key);
        long num = 0;
        for(String value: values) {
            boolean added = set.add(value);
            if (added) {
                num++;
            }
        }
        return num;
    }

    synchronized Boolean mockSismember(String key, String value) {
        return getStringSet(key).contains(value);
    }

    synchronized Long mockScard(String key) {
        return Long.valueOf(getStringSet(key).size());
    }

    synchronized ScanResult<String> mockSscan(String key, String cursor, ScanParams scanParams) {
        Set<String> set = getStringSet(key);
        return new ScanResult<String>(ScanParams.SCAN_POINTER_START, new ArrayList<>(set));
    }

}