package org.obapanel.jedis.interruptinglocks;

import org.mockito.Mockito;
import org.obapanel.jedis.common.test.TransactionOrder;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBase;
import redis.clients.jedis.params.SetParams;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.obapanel.jedis.common.test.TTL.wrapTTL;

/**
 * Mock of jedis methods used by the lock
 *
 * To allow or disallow unit tests
 * change the UNIT_TEST_CYCLES variable to 1 or more (allow) or zero (disallow)
 */
public class MockOfJedis {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockOfJedis.class);

    public static final String CLIENT_RESPONSE_OK = "OK";
    public static final String CLIENT_RESPONSE_KO = "KO";


    // Zero to prevent some unit test
    // One to one pass
    // More to more passes
    public static final int UNIT_TEST_CYCLES = 1;

    static boolean unitTestEnabled(){
        return UNIT_TEST_CYCLES > 0;
    }


    static boolean checkLock(IJedisLock jedisLock){
        LOGGER.info("interruptingLock.isLocked() " + jedisLock.isLocked() + " for thread " + Thread.currentThread().getName());
        if (jedisLock.isLocked()) {
            LOGGER.debug("LOCKED");
            return true;
        } else {
            IllegalStateException ise =  new IllegalStateException("LOCK NOT ADQUIRED isLocked " + jedisLock.isLocked());
            LOGGER.error("ERROR LOCK NOT ADQUIRED e {} ", ise.getMessage(), ise);
            throw ise;
        }
    }

    static boolean checkLock(java.util.concurrent.locks.Lock lock){
        if (lock instanceof  org.obapanel.jedis.interruptinglocks.Lock) {
            org.obapanel.jedis.interruptinglocks.Lock jedisLock = (org.obapanel.jedis.interruptinglocks.Lock) lock;
            LOGGER.info("interruptingLock.isLocked() " + jedisLock.isLocked() + " for thread " + Thread.currentThread().getName());
            if (jedisLock.isLocked()) {
                LOGGER.debug("LOCKED");
                return true;
            } else {
                IllegalStateException ise =  new IllegalStateException("LOCK NOT ADQUIRED isLocked " + jedisLock.isLocked());
                LOGGER.error("ERROR LOCK NOT ADQUIRED e {} ", ise.getMessage(), ise);
                throw ise;
            }
        } else {
            return true;
        }
    }

    private final Jedis jedis;
    private final JedisPool jedisPool;
    private final List<TransactionOrder<String>> transactionActions = new ArrayList<>();
    private final Map<String, String> data = Collections.synchronizedMap(new HashMap<>());
    private final Timer timer;

    public MockOfJedis() {
        PowerMockito.suppress(MemberMatcher.methodsDeclaredIn(TransactionBase.class));

        timer = new Timer();
        jedis = Mockito.mock(Jedis.class);
        jedisPool = Mockito.mock(JedisPool.class);
        Transaction transaction = PowerMockito.mock(Transaction.class);


        Mockito.when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            SetParams setParams = ioc.getArgument(2);
            return mockSet(key, value, setParams);

        });
        Mockito.when(jedisPool.getResource()).thenReturn(jedis);
        Mockito.when(jedis.get(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockGet(key);
        });
        Mockito.when(jedis.eval(anyString(),any(List.class), any(List.class))).thenAnswer(ioc -> {
            String script = ioc.getArgument(0);
            List<String> keys = ioc.getArgument(1);
            List<String> values = ioc.getArgument(2);
            return mockEval(script, keys, values);
        });
        Mockito.when(jedis.multi()).thenReturn(transaction);
        Mockito.when(transaction.get(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockTransactionGet(key);
        });
        Mockito.when(transaction.set(anyString(), anyString(), any(SetParams.class))).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            SetParams setParams = ioc.getArgument(2);
            return mockTransactionSet(key, value, setParams);
        });
        Mockito.when(transaction.exec()).thenAnswer(ioc -> mockTransactionExec());
    }

    private synchronized String mockGet(String key) {
        return data.get(key);
    }

    private synchronized Object mockEval(String script, List<String> keys, List<String> values) {
        Object response = null;
        if (script.equalsIgnoreCase(JedisLock.UNLOCK_LUA_SCRIPT)) {
            if (values.get(0).equalsIgnoreCase(data.get(keys.get(0)))){
                String removed = data.remove(keys.get(0));
                response = removed != null ? 1 : 0;
            }
        }
        return response;
    }

    private synchronized String mockSet(final String key, String value, SetParams setParams) {
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

    private synchronized Response<String> mockTransactionGet(String key){
        TransactionOrder<String> transactionOrder = new TransactionOrder<>(() -> mockGet(key));
        transactionActions.add(transactionOrder);
        return transactionOrder.getResponse();
    }

    private synchronized Response<String> mockTransactionSet(String key, String value, SetParams setParams){
        TransactionOrder<String> transactionOrder = new TransactionOrder<>(() -> mockSet(key, value, setParams));
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


    public Jedis getJedis(){
        return jedis;
    }

    public JedisPool getJedisPool(){
        return jedisPool;
    }

    public synchronized void clearData(){
        data.clear();
        transactionActions.clear();
    }

    public synchronized Map<String,String> getCurrentData() {
        return new HashMap<>(data);
    }

    boolean isSetParamsNX(SetParams setParams) {
        boolean result = false;
        for(byte[] b: setParams.getByteParams()){
            String s = new String(b);
            if ("nx".equalsIgnoreCase(s)){
                result = true;
                break;
            }
        }
        return result;
    }

    Long getExpireTimePX(SetParams setParams) {
        return setParams.getParam("px");
    }


    // To allow deeper testing
    @SuppressWarnings("All")
    public static String getJedisLockValue(JedisLock jedisLock) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method privateMethod = JedisLock.class.getDeclaredMethod("getValue", null);
        privateMethod.setAccessible(true);
        return (String) privateMethod.invoke(jedisLock, null);
    }

}
