package org.obapanel.jedis.cache.javaxcache;

import redis.clients.jedis.*;
import redis.clients.jedis.params.SetParams;

import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

public class RedisCache implements Cache<String,String> {


    private final JedisPool jedisPool;
    private final String name;
    private final RedisCacheConfiguration configuration;
    private final RedisCacheManager cacheManager;

    RedisCache(JedisPool jedisPool, String name, RedisCacheConfiguration redisCacheConfiguration) {
        this.jedisPool = jedisPool;
        this.name = name;
        this.configuration = redisCacheConfiguration;
        this.cacheManager = null;
    }

    RedisCache(String name, RedisCacheConfiguration redisCacheConfiguration, RedisCacheManager cacheManager) {
        this.jedisPool = cacheManager.getJedisPool();
        this.name = name;
        this.configuration = redisCacheConfiguration;
        this.cacheManager = cacheManager;
    }

    @Override
    public RedisCacheManager getCacheManager() {
        return cacheManager;
    }

    @Override
    public String getName() {
        return name;
    }

    public RedisCacheConfiguration getConfiguration() {
        return configuration;
    }

    String dataKey(String key) {
        return name + ":" + key;
    }

    <K> K underPool(Function<Jedis, K> action) {
        try (Jedis jedis = jedisPool.getResource()){
            return action.apply(jedis);
        }
    }

    void underPool(Consumer<Jedis> action) {
        try (Jedis jedis = jedisPool.getResource()){
            action.accept(jedis);
        }
    }

    @Override
    public String get(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key);
        }
    }

    @Override
    public Map<String, String> getAll(Set<? extends String> keys) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, Response<String>> responses = new HashMap<>();
            Transaction t = jedis.multi();
            for(String key: keys) {
                responses.put(key, t.get(key));
            }
            t.exec();
            //            return responses.entrySet().stream().
//                    filter( entry -> entry.getValue().get() != null ).
//                    collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            Map<String, String> result = new HashMap<>();
            for(Map.Entry<String, Response<String>> entry: responses.entrySet()) {
                if (entry.getValue() != null && entry.getValue().get() != null) {
                    result.put(entry.getKey(), entry.getValue().get());
                }
            }
            return result;
        }
    }

    @Override
    public boolean containsKey(String key) {
        return get(key) != null;
    }

    @Override
    public void loadAll(Set<? extends String> keys, boolean replaceExistingValues, CompletionListener completionListener) {

    }

    @Override
    public void put(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(key, value);
        }
    }

    @Override
    public String getAndPut(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction t = jedis.multi();
            Response<String> response = t.get(key);
            t.set(key, value);
            t.exec();
            return response.get();
        }
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> map) {
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction t = jedis.multi();
            map.forEach(t::set);
            t.exec();
        }
    }

    @Override
    public boolean putIfAbsent(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            String result = jedis.set(key, value, new SetParams().nx());
            return result != null; //TODO
        }
    }

    @Override
    public boolean remove(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction t = jedis.multi();
            Response<String> previous = t.get(key);
            t.del(key);
            t.exec();
            return previous.get() != null;
        }
    }

    @Override
    public boolean remove(String key, String oldValue) {
        //Better with script
        try (Jedis jedis = jedisPool.getResource()) {
            String current = jedis.get(key);
            if (current != null && current.equals(oldValue)) {
                jedis.del(key);
                return true;
            }  else {
                return false;
            }
        }
    }

    @Override
    public String getAndRemove(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction t = jedis.multi();
            Response<String> previous = t.get(key);
            t.del(key);
            t.exec();
            return previous.get();
        }
    }

    @Override
    public boolean replace(String key, String oldValue, String newValue) {
        //Better with script
        try (Jedis jedis = jedisPool.getResource()) {
            String current = jedis.get(key);
            if (current != null && current.equals(oldValue)) {
                jedis.set(key, newValue);
                return true;
            }  else {
                return false;
            }
        }
    }

    @Override
    public boolean replace(String key, String value) {
        //Better with script
        try (Jedis jedis = jedisPool.getResource()) {
            String current = jedis.get(key);
            if (current != null) {
                jedis.set(key, value);
                return true;
            }  else {
                return false;
            }
        }
    }

    @Override
    public String getAndReplace(String key, String value) {
        //Better with script
        try (Jedis jedis = jedisPool.getResource()) {
            String current = jedis.get(key);
            if (current != null) {
                jedis.set(key, value);
                return current;
            }  else {
                return null;
            }
        }
    }

    @Override
    public void removeAll(Set<? extends String> keys) {
        try (Jedis jedis = jedisPool.getResource()) {
            String[] keysAsArray = keys.toArray(new String[]{});
            jedis.del(keysAsArray);
        }
    }

    @Override
    public void removeAll() {
        try (Jedis jedis = jedisPool.getResource()) {
            String[] keysAsArray = doScan().toArray(new String[]{});
            jedis.del(keysAsArray);
        }
    }

    @Override
    public void clear() {

    }

    @Override
    public <C extends Configuration<String, String>> C getConfiguration(Class<C> clazz) {
        return (C) configuration;
    }

    @Override
    public <T> T invoke(String key, EntryProcessor<String, String, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        return null;
    }

    @Override
    public <T> Map<String, EntryProcessorResult<T>> invokeAll(Set<? extends String> keys, EntryProcessor<String, String, T> entryProcessor, Object... arguments) {
        return null;
    }


    @Override
    public void close() {

    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return null;
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<String, String> cacheEntryListenerConfiguration) {

    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<String, String> cacheEntryListenerConfiguration) {

    }

    @Override
    public Iterator<Entry<String, String>> iterator() {
        return null;
    }

    private Set<String> doScan() {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> keys = new HashSet<>();
            ScanParams scanParams = new ScanParams(); // Scan on two-by-two responses
            String cursor = ScanParams.SCAN_POINTER_START;
            do {
                ScanResult<String> partialResult =  jedis.scan(cursor, scanParams);
                cursor = partialResult.getCursor();
                keys.addAll(partialResult.getResult());
            }  while(!cursor.equals(ScanParams.SCAN_POINTER_START));
            return keys;
        }
    }
}
