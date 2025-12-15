package org.oba.jedis.extra.utils.interruptinglocks;

import org.mockito.Mockito;
import org.oba.jedis.extra.utils.lock.IJedisLock;
import org.oba.jedis.extra.utils.test.TTL;
import org.oba.jedis.extra.utils.test.TransactionOrder;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha1;
import org.powermock.api.mockito.PowerMockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.args.ExpiryOption;
import redis.clients.jedis.params.SetParams;

import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.*;
import static org.oba.jedis.extra.utils.test.TestingUtils.extractSetParamsExpireTimePX;
import static org.oba.jedis.extra.utils.test.TestingUtils.isSetParamsNX;

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




    private final Jedis jedis;
    private final JedisPool jedisPool;
    private final List<TransactionOrder<String>> transactionActions = new ArrayList<>();
    private final Map<String, String> data = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, TimerTaskWithMoment> dataTimerTask = Collections.synchronizedMap(new HashMap<>());
    private final Timer timer;

    public MockOfJedis() {


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
        Mockito.when(jedis.scriptLoad(anyString())).thenAnswer( ioc -> {
            String script = ioc.getArgument(0, String.class);
            return ScriptEvalSha1.sha1(script);
        });
        Mockito.when(jedis.evalsha(anyString(), any(List.class), any(List.class))).thenAnswer( ioc -> {
            String name = ioc.getArgument(0, String.class);
            List<String> keys = ioc.getArgument(1, List.class);
            List<String> args = ioc.getArgument(2, List.class);
            return mockEvalsha(keys, args);
        });
        Mockito.when(jedis.pexpire(anyString(), anyLong(), any(ExpiryOption.class))).thenAnswer( ioc -> {
            String name = ioc.getArgument(0, String.class);
            long time = ioc.getArgument(1, Long.class);
            ExpiryOption expiryOption = ioc.getArgument(2, ExpiryOption.class);
            return mockEvalPExpire(name, time, expiryOption);
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

    //TODO correct ?
    private Object mockEvalPExpire(String name, long timeInMillis, ExpiryOption expiryOption) {
        long response = 0L;
        if (data.get(name) != null) {
            TimerTaskWithMoment timerTask = dataTimerTask.get(name);
            if (timerTask != null) {
                timerTask.cancelTask();
                long newTime = timerTask.addTime(timeInMillis);
                timer.schedule(timerTask.createTask(), newTime);
                response = 1L;
            } else {
                LOGGER.warn("no timertask for {}", name);
            }
        } else {
            LOGGER.debug("no value for {}", name);
        }
        return response;
    }

    private synchronized String mockGet(String key) {
        return data.get(key);
    }

    private synchronized Object mockEvalsha(List<String> keys, List<String> values) {
        long response = 0L;
        String key = keys.get(0);
        if (values.get(0).equalsIgnoreCase(data.get(key)) ){
            String removed = data.remove(key);
            if (removed != null) {
                response = 1L;
                TimerTaskWithMoment timerTask = dataTimerTask.remove(key);
                if (timerTask != null) {
                    timerTask.cancelTask();
                }
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
            Long expireTime = extractSetParamsExpireTimePX(setParams);
            if (expireTime != null){
                TimerTaskWithMoment timerTask = new TimerTaskWithMoment(expireTime, () -> data.remove(key));
                timer.schedule(timerTask.createTask(), expireTime);
                dataTimerTask.put(key, timerTask);
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
        dataTimerTask.values().forEach(TimerTaskWithMoment::cancelTask);
        dataTimerTask.clear();
        transactionActions.clear();
    }

    public synchronized Map<String,String> getCurrentData() {
        return new HashMap<>(data);
    }

    static class TimerTaskWithMoment {
        private long expireTime;
        private final Runnable runnable;
        private TimerTask timerTask;

        TimerTaskWithMoment(long time, Runnable runnable){
            this.expireTime = time + System.currentTimeMillis();
            this.runnable = runnable;
        }

        TimerTask createTask() {
            timerTask = TTL.wrapTTL(runnable);
            return timerTask;
        }

        void cancelTask() {
            timerTask.cancel();
            timerTask = null;
        }

        long addTime(long addedTimeMillis) {
            long restTime = expireTime - System.currentTimeMillis();
            long newTime = addedTimeMillis + restTime;
            expireTime = newTime + System.currentTimeMillis();
            return newTime;
        }
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
}
