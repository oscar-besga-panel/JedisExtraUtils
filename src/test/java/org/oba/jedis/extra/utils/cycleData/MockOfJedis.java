package org.oba.jedis.extra.utils.cycleData;

import org.mockito.Mockito;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha1;
import org.oba.jedis.extra.utils.utils.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private final JedisPooled jedisPooled;
    private final Map<String, Map<String,String>> data = Collections.synchronizedMap(new HashMap<>());

    private TriFunction<String, List<String>, List<String>, Object> doWithEvalSha;

    public MockOfJedis() {

        jedisPooled = Mockito.mock(JedisPooled.class);
        when(jedisPooled.exists(anyString())).thenAnswer( ioc -> {
            String name = ioc.getArgument(0, String.class);
            return exists(name);
        });
        when(jedisPooled.del(anyString())).thenAnswer( ioc -> {
            String name = ioc.getArgument(0, String.class);
            return delete(name);
        });
        when(jedisPooled.hset(anyString(), any(Map.class))).thenAnswer( ioc ->  {
            String name = ioc.getArgument(0, String.class);
            Map<String, String> map = ioc.getArgument(1, Map.class);
            return hset(name, map);
        });
        when(jedisPooled.hset(anyString(), anyString(), anyString())).thenAnswer( ioc ->  {
            String name = ioc.getArgument(0, String.class);
            String key = ioc.getArgument(1, String.class);
            String value = ioc.getArgument(2, String.class);
            return hset(name, key, value);
        });
        when(jedisPooled.scriptLoad(anyString())).thenAnswer( ioc -> {
            String script = ioc.getArgument(0, String.class);
            return ScriptEvalSha1.sha1(script);
        });
        when(jedisPooled.evalsha(anyString(), any(List.class), any(List.class))).thenAnswer( ioc -> {
            String name = ioc.getArgument(0, String.class);
            List<String> keys = ioc.getArgument(1, List.class);
            List<String> args = ioc.getArgument(2, List.class);
            return executeSha(name, keys, args);
        });
    }


    private Object executeSha(String name, List<String> keys, List<String> args) {
        return doWithEvalSha != null ? doWithEvalSha.apply(name, keys, args) : null;
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

    public JedisPooled getJedisPooled(){
        return jedisPooled;
    }

    public synchronized void clearData(){
        data.clear();
        doWithEvalSha = null;
    }

    public Map<String, String> getData(String name) {
        return data.get(name);
    }



}
