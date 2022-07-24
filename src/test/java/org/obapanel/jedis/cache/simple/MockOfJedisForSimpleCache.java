package org.obapanel.jedis.cache.simple;

import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.obapanel.jedis.common.test.TransactionOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.params.SetParams;

import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.obapanel.jedis.common.test.TTL.wrapTTL;

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

    private final Jedis jedis;
    private final JedisPool jedisPool;
    private final Map<String, String> data = Collections.synchronizedMap(new HashMap<>());
    private final List<TransactionOrder<?>> transactionActions = new ArrayList<>();
    private final Timer timer;


    public MockOfJedisForSimpleCache() {
        timer = new Timer();

        jedis = Mockito.mock(Jedis.class);
        jedisPool = Mockito.mock(JedisPool.class);
        when(jedisPool.getResource()).thenReturn(jedis);
        Transaction transaction = Mockito.mock(Transaction.class);
        when(jedis.multi()).thenReturn(transaction);

        when(jedis.exists(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockExists(key);
        });
        when(jedis.get(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockGet(key);
        });
        when(jedis.set(anyString(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            return mockSet(key, value);
        });
        when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            SetParams setParams = ioc.getArgument(2);
            return mockSet(key, value, setParams);
        });
        when(jedis.del(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockDelOne(key);
        });
        when(jedis.del(ArgumentMatchers.<String[]>any())).thenAnswer(ioc -> {
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
        when(jedis.scan(anyString(), any(ScanParams.class))).thenAnswer(ioc -> {
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
        Mockito.when(transaction.exec()).thenAnswer(ioc -> mockTransactionExec());

        when(jedis.eval(anyString(),any(List.class), any(List.class))).thenAnswer(ioc -> null);

    }


    Jedis getJedis(){
        return jedis;
    }

    JedisPool getJedisPool() {
        return jedisPool;
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
            Long expireTime = getExpireTimePX(setParams);
            if (expireTime != null){
                timer.schedule(wrapTTL(() -> data.remove(key)),expireTime);
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

    boolean isSetParamsNX(SetParams setParams) {
        boolean result = false;
        for(byte[] b: setParams.getByteParams()){
            String s = new String(b);
            if ("nx".equalsIgnoreCase(s)){
                result = true;
            }
        }
        return result;
    }

    Long getExpireTimePX(SetParams setParams) {
        return setParams.getParam("px");
    }

    public static String extractPatternFromScanParams(ScanParams scanParams) {
        boolean nextIsPattern = false;
        String pattern = "";
        for(byte[] p : scanParams.getParams()) {
            String s = new String(p).intern();
            if (nextIsPattern) {
                pattern = s;
            }
            nextIsPattern = Protocol.Keyword.MATCH.name().equalsIgnoreCase(s);
        }
        if (pattern.equals("*")) {
            pattern = ".*";
        }
        if (pattern.endsWith("*")) {
            pattern = pattern.replace("*", ".*");
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
