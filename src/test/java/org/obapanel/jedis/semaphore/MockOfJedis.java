package org.obapanel.jedis.semaphore;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Mock of jedis methods used by the lock
 */
public class MockOfJedis {

    private static final Logger LOG = LoggerFactory.getLogger(MockOfJedis.class);

    public static final String CLIENT_RESPONSE_OK = "OK";
    public static final String CLIENT_RESPONSE_KO = "KO";


    // Zero to prevent some unit test
    // One to one pass
    // More to more passes
    static final int INTEGRATION_TEST_CYCLES = 1;

    static boolean integrationTestEnabled(){
        return INTEGRATION_TEST_CYCLES > 0;
    }

    private JedisPool jedisPool;
    private Jedis jedis;
    private Map<String, String> data = Collections.synchronizedMap(new HashMap<>());
    private Timer timer;

    public MockOfJedis() {
        timer = new Timer();

        jedis = Mockito.mock(Jedis.class);

        Mockito.when(jedis.get(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockGet(key);
        });
        Mockito.when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            SetParams setParams = ioc.getArgument(2);
            return mockSet(key, value, setParams);
        });
        Mockito.when(jedis.incrBy(anyString(), anyLong())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            long value = ioc.getArgument(1);
            mockIncrBy(key, value);
            return null;
        });
        Mockito.when(jedis.eval(anyString(),any(List.class), any(List.class))).thenAnswer(ioc -> {
            String script = ioc.getArgument(0);
            List<String> keys = ioc.getArgument(1);
            List<String> values = ioc.getArgument(2);
            return mockEval(script, keys, values);
        });
        jedisPool = Mockito.mock(JedisPool.class);
        Mockito.when(jedisPool.getResource()).thenReturn(jedis);
    }

    private synchronized void mockIncrBy(String key, long value) {
        if (data.containsKey(key)) {
            long permitsAvalible = data.containsKey(key) ? Long.parseLong(data.get(key)) : -1;
            permitsAvalible = permitsAvalible + value;
            data.put(key, String.valueOf(permitsAvalible));
        }
    }

    private synchronized String mockGet(String key) {
        return data.get(key);
    }

    private synchronized Object mockEval(String script, List<String> keys, List<String> values) {
        Object response = null;
        if (script.equalsIgnoreCase(JedisAdvancedSemaphore.SEMAPHORE_LUA_SCRIPT)) {
            response = mockEvalSemaphoreLuaScript(keys, values);
        }
        return response;
    }

    private synchronized Object mockEvalSemaphoreLuaScript(List<String> keys, List<String> values) {
        Object response;
        String key = keys.get(0);
        long permitsToTake = Long.parseLong(values.get(0));
        long permitsAvalible =  data.containsKey(key) ? Long.parseLong(data.get(key)) : -1;
        if (data.containsKey(key) && permitsAvalible >= permitsToTake) {
            permitsAvalible = permitsAvalible - permitsToTake;
            data.put(key, String.valueOf(permitsAvalible));
            response = "true";
        }  else {
            response = "false";
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

    public Jedis getJedis(){
        return jedis;
    }

    public JedisPool getJedisPool(){
        return jedisPool;
    }

    public synchronized void clearData(){
        data.clear();
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
            }
        }
        return result;
    }

    Long getExpireTimePX(SetParams setParams) {
        return (Long) setParams.getParam("px");
    }



    private static TimerTask wrapTTL(Runnable r) {
        return new TTL(r);
    }

    // TimerTaskLamda
    private static class TTL extends TimerTask {

        private Runnable runnable;

        public TTL(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void run() {
            runnable.run();
        }
    }




}
