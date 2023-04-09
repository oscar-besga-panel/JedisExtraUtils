package org.oba.jedis.extra.utils.collections;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.oba.jedis.extra.utils.test.TransactionOrder;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBase;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Mock of jedis methods used by the lock
 */
public class MockOfJedisForSet {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockOfJedisForList.class);

    private final Jedis jedis;
    private final JedisPool jedisPool;
    private final Map<String, Object> data = Collections.synchronizedMap(new HashMap<>());

    public MockOfJedisForSet() {
        PowerMockito.suppress(MemberMatcher.methodsDeclaredIn(TransactionBase.class));


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
            return mockDel(key);
        });
        when(transaction.del(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return TransactionOrder.quickReponseExecuted(mockDel(key));
        });
        when(jedis.sadd(anyString(), any())).thenAnswer(this::iocSadd);
        when(transaction.sadd(anyString(), any())).thenAnswer( ioc ->
                TransactionOrder.quickReponseExecuted(iocSadd(ioc))
        );
        when(jedis.sismember(anyString(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            return mockSismember(key, value);
        });
        when(jedis.scard(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockScard(key);
        });
        when(jedis.srem(anyString(), any())).thenAnswer(ioc -> {
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
            return mockSrem(key, value);
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
        PowerMockito.when(transaction.exec()).thenAnswer(ioc -> mockTransactionExec());
    }

    private Long iocSadd(InvocationOnMock ioc){
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
        if (set.isEmpty()) {
            return new ScanResult<>(ScanParams.SCAN_POINTER_START, new ArrayList<>());
        } else if (ScanParams.SCAN_POINTER_START.equals(cursor)) {
            return new ScanResult<>("999", new ArrayList<>(set));
        } else {
            return new ScanResult<>(ScanParams.SCAN_POINTER_START, new ArrayList<>());
        }
    }

    synchronized Long mockSrem(String key, Object value) {
        if (value instanceof String) {
            return doRem(key, new String[] {(String) value});
        } else if (value instanceof String[]) {
            return doRem(key, (String[]) value);
        } else {
            return 0L;
        }
    }

    synchronized Long doRem(String key, String[] values) {
        Set<String> set = getStringSet(key);
        long num = 0;
        for(String value: values) {
            boolean removed = set.remove(value);
            if (removed) {
                num++;
            }
        }
        return num;
    }

    private Object mockTransactionExec() {
        LOGGER.debug("mockTransactionExec do nothing");
        return new ArrayList<Object>(0);
    }

}