package org.oba.jedis.extra.utils.countdownlatch;

import org.mockito.Mockito;
import org.oba.jedis.extra.utils.test.TTL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Mock of jedis methods used by the lock
 */
public class MockOfJedis {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockOfJedis.class);

    public static final String CLIENT_RESPONSE_OK = "OK";
    public static final String CLIENT_RESPONSE_KO = "KO";


    // Zero to prevent some unit test
    // One to one pass
    // More to more passes
    static final int UNIT_TEST_CYCLES = 1;

    static boolean unitTestEnabled(){
        return UNIT_TEST_CYCLES > 0;
    }

    private final Jedis jedis;
    private final JedisPool jedisPool;
    private final Map<String, String> data = Collections.synchronizedMap(new HashMap<>());
    private final Timer timer;

    public MockOfJedis() {
        timer = new Timer();


        jedis = Mockito.mock(Jedis.class);
        jedisPool = Mockito.mock(JedisPool.class);
        Mockito.when(jedisPool.getResource()).thenReturn(jedis);
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
        Mockito.when(jedis.del(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockDel(key);
        });
        Mockito.when(jedis.decr(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockDecr(key);
        });

    }


    private synchronized String mockGet(String key) {
        return data.get(key);
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
                timer.schedule(TTL.wrapTTL(() -> data.remove(key)),expireTime);
            }
            return  CLIENT_RESPONSE_OK;
        } else {
            return  CLIENT_RESPONSE_KO;
        }
    }

    private synchronized Long mockDel(String key) {
        if (data.containsKey(key)) {
            data.remove(key);
            return 1L;
        } else {
            return 0L;
        }
    }

    private synchronized Long mockDecr(String key) {
        if (data.containsKey(key)) {
            long count = data.containsKey(key) ? Long.parseLong(data.get(key)) : -1;
            count--;
            data.put(key, String.valueOf(count));
            return count;
        } else {
            return null;
        }
    }


    public Jedis getJedis(){
        return jedis;
    }

    public JedisPool getJedisPool() {
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
        return setParams.getParam("px");
    }





}
