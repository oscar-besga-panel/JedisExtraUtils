package org.oba.jedis.extra.utils.rateLimiter;

import org.mockito.Mockito;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha1;
import org.oba.jedis.extra.utils.utils.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.when;

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

    private final JedisPool jedisPool;
    private final Jedis jedis;
    private final Map<String, Map<String,String>> data = Collections.synchronizedMap(new HashMap<>());
    private final Timer timer;

    private TriFunction<String, List<String>, List<String>, Object> doWithEvalSha;

    public MockOfJedis() {
        timer = new Timer();

        jedis = Mockito.mock(Jedis.class);
        jedisPool = Mockito.mock(JedisPool.class);
        when(jedis.exists(anyString())).thenAnswer( ioc -> {
            String name = ioc.getArgument(0, String.class);
            return exists(name);
        });
        when(jedis.del(anyString())).thenAnswer( ioc -> {
            String name = ioc.getArgument(0, String.class);
            return delete(name);
        });
        when(jedis.hset(anyString(), any(Map.class))).thenAnswer( ioc ->  {
            String name = ioc.getArgument(0, String.class);
            Map<String, String> map = ioc.getArgument(1, Map.class);
            return hset(name, map);
        });
        when(jedis.hset(anyString(), anyString(), anyString())).thenAnswer( ioc ->  {
            String name = ioc.getArgument(0, String.class);
            String key = ioc.getArgument(1, String.class);
            String value = ioc.getArgument(2, String.class);
            return hset(name, key, value);
        });
        when(jedis.time()).thenAnswer(ioc -> time());
        when(jedis.scriptLoad(anyString())).thenAnswer( ioc -> {
            String script = ioc.getArgument(0, String.class);
            return ScriptEvalSha1.sha1(script);
        });
        when(jedis.evalsha(anyString(), any(List.class), any(List.class))).thenAnswer( ioc -> {
            String name = ioc.getArgument(0, String.class);
            List<String> keys = ioc.getArgument(1, List.class);
            List<String> args = ioc.getArgument(2, List.class);
            return executeSha(name, keys, args);
        });
        Mockito.when(jedisPool.getResource()).thenReturn(jedis);
    }


    private Object executeSha(String name, List<String> keys, List<String> args) {
        return doWithEvalSha != null ? doWithEvalSha.apply(name, keys, args) : null;
    }

    private List<String> time() {
        long seconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        return Arrays.asList(Long.toString(seconds),"0");
    }

    private Boolean exists(String name) {
        return data.containsKey(name);
    }

    private Long hset(String name, String key, String value) {
        data.computeIfAbsent(name, k -> new HashMap<>()).put(key, value);
        return 1L;
    }

    private Long hset(String name, Map<String, String> map) {
        data.computeIfAbsent(name, k -> new HashMap<>()).putAll(map);
        return 1L;
    }

    private Object delete(String name) {
        Map<String, String> removed = data.remove(name);
        return removed != null ? 1L : 0L;
    }


    public void setDoWithEvalSha(TriFunction<String, List<String>, List<String>, Object> doWithEvalSha) {
        this.doWithEvalSha = doWithEvalSha;
    }

    public Jedis getJedis(){
        return jedis;
    }

    public JedisPool getJedisPool(){
        return jedisPool;
    }

    public synchronized void clearData(){
        data.clear();
        doWithEvalSha = null;
    }

    public Map<String, String> getData(String name) {
        return data.get(name);
    }



}
