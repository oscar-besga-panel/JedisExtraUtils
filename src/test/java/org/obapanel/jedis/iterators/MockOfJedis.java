package org.obapanel.jedis.iterators;

import org.mockito.Mockito;
import org.obapanel.jedis.semaphore.JedisAdvancedSemaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.params.SetParams;

import java.util.*;

import static org.mockito.ArgumentMatchers.*;
import static org.obapanel.jedis.common.test.TTL.wrapTTL;

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
    static final int UNIT_TEST_CYCLES = 1;

    static boolean unitTestEnabled(){
        return UNIT_TEST_CYCLES > 0;
    }

    private final JedisPool jedisPool;
    private final Jedis jedis;
    private final Map<String, String> data = Collections.synchronizedMap(new HashMap<>());
    private final Timer timer;

    public MockOfJedis() {
        timer = new Timer();
        jedis = Mockito.mock(Jedis.class);
        jedisPool = Mockito.mock(JedisPool.class);
        Mockito.when(jedisPool.getResource()).thenReturn(jedis);
        Mockito.when(jedis.del(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockDel(key);
        });
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
        Mockito.when(jedis.scan(anyString(), any(ScanParams.class))).thenAnswer(ioc -> {
            String pattern = ioc.getArgument(0);
            ScanParams scanParams = ioc.getArgument(1);
            return mockScan(pattern, scanParams);
        });
        Mockito.when(jedis.scan(anyString())).thenAnswer(ioc -> {
            String pattern = ioc.getArgument(0);
            ScanParams scanParams = new ScanParams();
            return mockScan(pattern, scanParams);
        });
    }

    private ScanResult<String> mockScan(String pattern, ScanParams scanParams) {
        List<String> results = new ArrayList<>(data.keySet());
        return new ScanResult<String>(ScanParams.SCAN_POINTER_START, results);
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
                timer.schedule(wrapTTL(() -> data.remove(key)),expireTime);
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
                break;
            }
        }
        return result;
    }

    Long getExpireTimePX(SetParams setParams) {
        return setParams.getParam("px");
    }


 }
