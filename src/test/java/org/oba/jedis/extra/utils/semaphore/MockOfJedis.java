package org.oba.jedis.extra.utils.semaphore;

import org.mockito.Mockito;
import org.oba.jedis.extra.utils.test.TTL;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.SetParams;

import java.util.*;

import static org.mockito.ArgumentMatchers.*;
import static org.oba.jedis.extra.utils.test.TestingUtils.extractSetParamsExpireTimePX;
import static org.oba.jedis.extra.utils.test.TestingUtils.isSetParamsNX;
import static org.powermock.api.mockito.PowerMockito.when;

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

    private final JedisPooled jedisPooled;
    private final Jedis jedis;
    private final Map<String, String> data = Collections.synchronizedMap(new HashMap<>());
    private final Timer timer;

    public MockOfJedis() {
        timer = new Timer();

        jedis = Mockito.mock(Jedis.class);
        jedisPooled = Mockito.mock(JedisPooled.class);
//        Mockito.when(jedisPooled.getResource()).thenReturn(jedis);
//
        Mockito.when(jedisPooled.get(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockGet(key);
        });
        Mockito.when(jedisPooled.set(anyString(), anyString(), any(SetParams.class))).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            SetParams setParams = ioc.getArgument(2);
            return mockSet(key, value, setParams);
        });
        Mockito.when(jedisPooled.incrBy(anyString(), anyLong())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            long value = ioc.getArgument(1);
            return mockIncrBy(key, value);
        });
        Mockito.when(jedisPooled.del(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockDel(key);
        });
        when(jedisPooled.scriptLoad(anyString())).thenAnswer( ioc -> {
            String script = ioc.getArgument(0, String.class);
            return ScriptEvalSha1.sha1(script);
        });
        when(jedisPooled.evalsha(anyString(), any(List.class), any(List.class))).thenAnswer( ioc -> {
            String name = ioc.getArgument(0, String.class);
            List<String> keys = ioc.getArgument(1, List.class);
            List<String> args = ioc.getArgument(2, List.class);
            return mockEvalSemaphoreLuaScript(keys, args);
        });
        Mockito.when(jedisPooled.eval(anyString(),any(List.class), any(List.class))).thenAnswer(ioc -> {
            String script = ioc.getArgument(0);
            List<String> keys = ioc.getArgument(1);
            List<String> values = ioc.getArgument(2);
            return mockEvalSemaphoreLuaScript(keys, values);
        });

    }

    private synchronized Long mockIncrBy(String key, long value) {
        if (data.containsKey(key)) {
            long permitsAvalible = data.containsKey(key) ? Long.parseLong(data.get(key)) : -1;
            permitsAvalible = permitsAvalible + value;
            data.put(key, String.valueOf(permitsAvalible));
            return permitsAvalible;
        } else {
            return null;
        }
    }

    private synchronized String mockGet(String key) {
        return data.get(key);
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
            Long expireTime = extractSetParamsExpireTimePX(setParams);
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

//    public Jedis getJedis(){
//        return jedis;
//    }

    public JedisPooled getJedisPooled(){
        return jedisPooled;
    }

    public synchronized void clearData(){
        data.clear();
    }


    public synchronized Map<String,String> getCurrentData() {
        return new HashMap<>(data);
    }

 }
