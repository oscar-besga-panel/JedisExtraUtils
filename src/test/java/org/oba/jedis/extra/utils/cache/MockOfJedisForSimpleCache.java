package org.oba.jedis.extra.utils.cache;

import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.oba.jedis.extra.utils.test.TTL;
import org.oba.jedis.extra.utils.test.TransactionOrder;
import org.powermock.api.mockito.PowerMockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.oba.jedis.extra.utils.test.TestingUtils.extractSetParamsExpireTimePX;
import static org.oba.jedis.extra.utils.test.TestingUtils.isSetParamsNX;

/**
 * Mock of jedis methods used by the lock
 */
public class MockOfJedisForSimpleCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockOfJedisForSimpleCache.class);


    public static final String CLIENT_RESPONSE_OK = "OK";
    public static final String CLIENT_RESPONSE_KO = null;


    // Zero to prevent some unit test
    // One to one pass
    // More to more passes
    static final int UNIT_TEST_CYCLES_LIST = 1;

    static boolean unitTestEnabledForSimpleCache(){
        return UNIT_TEST_CYCLES_LIST > 0;
    }

    private final JedisPooled jedisPooled;
    private final Map<String, String> data = Collections.synchronizedMap(new HashMap<>());
    private final List<TransactionOrder<?>> transactionActions = new ArrayList<>();
    private final Timer timer;


    public MockOfJedisForSimpleCache() {

        timer = new Timer();

        jedisPooled = Mockito.mock(JedisPooled.class);
        Transaction transaction = PowerMockito.mock(Transaction.class);
        when(jedisPooled.multi()).thenReturn(transaction);

        when(jedisPooled.exists(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockExists(key);
        });
        when(jedisPooled.get(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockGet(key);
        });
        when(jedisPooled.set(anyString(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            return mockSet(key, value);
        });
        when(jedisPooled.set(anyString(), anyString(), any(SetParams.class))).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            SetParams setParams = ioc.getArgument(2);
            return mockSet(key, value, setParams);
        });
        when(jedisPooled.del(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockDelOne(key);
        });
        when(jedisPooled.del(ArgumentMatchers.<String[]>any())).thenAnswer(ioc -> {
            Object arg1 = ioc.getArgument(0);
            if (ioc.getArguments().length == 1 && arg1.getClass().isArray() && arg1.getClass().isAssignableFrom(String.class)) {
                return mockDel((String[]) arg1);
            } else if (ioc.getArguments().length == 1 && !arg1.getClass().isArray() && arg1.getClass().isAssignableFrom(String.class)) {
                return mockDelOne((String) arg1);
            } else if (ioc.getArguments().length > 1 && !arg1.getClass().isArray() && arg1.getClass().isAssignableFrom(String.class)) {
                return mockDel(fromObjectArray(ioc.getArguments()));
            } else {
                throw new UnsupportedOperationException("Mock jedis del. Dont know what is Object arg1: " + arg1);
            }
        });
        when(jedisPooled.scan(anyString(), any(ScanParams.class))).thenAnswer(ioc -> {
            String cursor = ioc.getArgument(0);
            ScanParams scanParams = ioc.getArgument(1);
            return mockScan(cursor, scanParams);
        });

        when(transaction.get(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockTransactionGet(key);
        });
        when(transaction.set(anyString(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            return mockTransactionSet(key, value);
        });
        when(transaction.set(anyString(), anyString(), any(SetParams.class))).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            SetParams setParams = ioc.getArgument(2);
            return mockTransactionSet(key, value, setParams);
        });
        when(transaction.del(anyString())).thenAnswer(ioc -> {
            String arg1 = ioc.getArgument(0);
            return mockTransactionDelOne(arg1);
        });
        when(transaction.del(ArgumentMatchers.<String[]>any())).thenAnswer(ioc -> {
            Object arg1 = ioc.getArgument(0);
            if (ioc.getArguments().length == 1 && arg1.getClass().isArray() && arg1.getClass().isAssignableFrom(String.class)) {
                return mockTransactionDel((String[]) arg1);
            } else if (ioc.getArguments().length == 1 && !arg1.getClass().isArray() && arg1.getClass().isAssignableFrom(String.class)) {
                return mockTransactionDelOne((String) arg1);
            } else if (ioc.getArguments().length > 1 && !arg1.getClass().isArray() && arg1.getClass().isAssignableFrom(String.class)) {
                return mockTransactionDel(fromObjectArray(ioc.getArguments()));
            } else {
                throw new UnsupportedOperationException("Mock jedis transaction del. Dont know what is Object arg1: " + arg1);
            }
        });
        PowerMockito.when(transaction.exec()).thenAnswer(ioc -> mockTransactionExec());

        when(jedisPooled.eval(anyString(),any(List.class), any(List.class))).thenAnswer(ioc -> null);

    }


    JedisPooled getJedisPooled() {
        return jedisPooled;
    }



    synchronized boolean mockExists(String key) {
        return data.containsKey(key);
    }

    synchronized String mockGet(String key) {
        return data.get(key);
    }

    synchronized String mockSet(final String key, String value) {
        return mockSet(key, value, new SetParams());
    }

    synchronized String mockSet(final String key, String value, SetParams setParams) {
        boolean insert = true;
        if (isSetParamsNX(setParams)) {
            insert = !data.containsKey(key);
        }
        if (insert) {
            data.put(key, value);
            Long expireTime = extractSetParamsExpireTimePX(setParams);
            if (expireTime != null){
                timer.schedule(TTL.wrapTTL(() -> data.remove(key)),expireTime);
            }
            return  CLIENT_RESPONSE_OK;
        } else {
            return  CLIENT_RESPONSE_KO;
        }
    }

    synchronized Long mockDelOne(String key) {
        return mockDel( new String[]{key} );
    }

    synchronized Long mockDel(String[] keys) {
        long result = 0L;
        for(String key: keys) {
            if (data.containsKey(key)) {
                data.remove(key);
                result++;
            }
        }
        return result;
    }

    private ScanResult<String> mockScan(String cursor, ScanParams scanParams) {
        if (!cursor.equals("0")) {
            LOGGER.warn("Cursor inited wirh value {}", cursor);
        }
        String pattern = extractPatternFromScanParams(scanParams);
        List<String> keys = data.keySet().stream().
                filter( k -> k.matches(pattern) ).
                collect(Collectors.toList());
        return new ScanResult<>("0", keys);
    }

    private synchronized Response<String> mockTransactionGet(String key){
        TransactionOrder<String> transactionOrder = new TransactionOrder<>(() -> mockGet(key));
        transactionActions.add(transactionOrder);
        return transactionOrder.getResponse();
    }

    private synchronized Response<String> mockTransactionSet(String key, String value){
        return mockTransactionSet(key, value, new SetParams());
    }

    private synchronized Response<String> mockTransactionSet(String key, String value, SetParams setParams){
        TransactionOrder<String> transactionOrder = new TransactionOrder<>(() -> mockSet(key, value, setParams));
        transactionActions.add(transactionOrder);
        return transactionOrder.getResponse();
    }

    private synchronized Response<Long> mockTransactionDelOne(String key){
        TransactionOrder<Long> transactionOrder = new TransactionOrder<>(() -> mockDelOne(key));
        transactionActions.add(transactionOrder);
        return transactionOrder.getResponse();
    }

    private synchronized Response<Long> mockTransactionDel(String... keys){
        TransactionOrder<Long> transactionOrder = new TransactionOrder<>(() -> mockDel(keys));
        transactionActions.add(transactionOrder);
        return transactionOrder.getResponse();
    }


    private synchronized List<Object> mockTransactionExec(){
        transactionActions.forEach(TransactionOrder::execute);
        List<Object> responses = transactionActions.stream().
                map(TransactionOrder::getResponse).
                collect(Collectors.toList());
        transactionActions.clear();
        return responses;
    }

    synchronized void clearData(){
        data.clear();
    }

    synchronized Map<String,Object> getCurrentData() {
        return new HashMap<>(data);
    }

    synchronized void put(String key, String element) {
        data.put(key, element);
    }

    public static String extractPatternFromScanParams(ScanParams scanParams) {
        String pattern = scanParams.match();
        if (pattern.equals("*")) {
            pattern = ".*";
        } else if (pattern.endsWith("*")) {
            pattern = pattern.replace("*",".*");
        } else if (pattern == null) {
            pattern = "";
        }
        return pattern;
    }


    static String[] fromObjectArray(Object[] data){
        String[] result = new String[data.length];
        for(int i = 0; i < data.length; i++){
            result[i] = data[i].toString();
        }
        return result;
    }

}
