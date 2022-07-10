package org.obapanel.jedis.cache.javaxcache;

import org.obapanel.jedis.iterators.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.params.SetParams;

import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class RedisCache implements Cache<String,String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisCache.class);


    private JedisPool jedisPool;

    private CacheLoader<String, String> cacheLoader;

    private CacheWriter<String, String> cacheWriter;

    private final String name;
    private final RedisCacheConfiguration configuration;
    private final RedisCacheManager cacheManager;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    RedisCache(String name, RedisCacheConfiguration redisCacheConfiguration, RedisCacheManager cacheManager) {
        this.jedisPool = cacheManager.getJedisPool();
        this.name = name;
        this.configuration = redisCacheConfiguration;
        this.cacheManager = cacheManager;
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public void setJedisPool(JedisPool jedisPool) {
        if (this.jedisPool != null) LOGGER.warn("JedisPool was not null, replacing");
        this.jedisPool = jedisPool;
    }

    public void clearJedisPool() {
        jedisPool = null;
    }

    public boolean hasJedisPool() {
        return jedisPool != null;
    }


    public CacheLoader<String, String> getCacheLoader() {
        return cacheLoader;
    }

    public void setCacheLoader(CacheLoader<String, String> cacheLoader) {
        if (this.cacheLoader != null) LOGGER.warn("CacheLoader was not null, replacing");
        this.cacheLoader = cacheLoader;
    }

    public void clearCacheLoader() {
        cacheLoader = null;
    }

    public boolean hasCacheLoader() {
        return cacheLoader != null;
    }


    public CacheWriter<String, String> getCacheWriter() {
        return cacheWriter;
    }

    public void setCacheWriter(CacheWriter<String, String> cacheWriter) {
        if (this.cacheWriter != null) LOGGER.warn("CacheWriter was not null, replacing");
        this.cacheWriter = cacheWriter;
    }

    public void clearCacheWriter() {
        cacheWriter = null;
    }

    public boolean hasCacheWriter() {
        return cacheWriter != null;
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

    public String resolveKey(String key) {
        if (key == null) throw new NullPointerException("Key must be not null");
        return name + ":" + key;
    }

    public String unresolveKey(String key) {
        if (key == null) throw new NullPointerException("Key must be not null");
        return key.replace(name + ":", "");
    }

    private String readThrougth(Jedis jedis, String key) {
        return readThrougth(jedis, key, null);
    }

    private String readThrougth(Jedis jedis, String key, String value) {
        if (value == null && cacheLoader != null) {
            value = cacheLoader.load(key);
            if (value != null) {
                jedis.set(resolveKey(key), value);
            }
        }
        return value;
    }

    @Override
    public String get(String key) {
        checkClosed();
        if (key == null) throw new NullPointerException("RedisCache.get key is null");
        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.get(resolveKey(key));
            value = readThrougth(jedis, key, value);
            return value;
        }
    }

    @Override
    public Map<String, String> getAll(Set<? extends String> keys) {
        checkClosed();
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, Response<String>> responses = new HashMap<>();
            Transaction t = jedis.multi();
            for(String key: keys) {
                responses.put(key, t.get(resolveKey(key)));
            }
            t.exec();
            Map<String, String> result = resolveTransactionEntries(jedis, responses);
            return result;
        }
    }

    private Map<String, String> resolveTransactionEntries(Jedis jedis, Map<String, Response<String>> responses) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, Response<String>> entry: responses.entrySet()) {
            String resolvedValue;
            if (entry.getValue() != null && entry.getValue().get() != null) {
                resolvedValue = entry.getValue().get();
            } else {
                resolvedValue = readThrougth(jedis, entry.getKey());
            }
            if (resolvedValue != null) {
                result.put(entry.getKey(), resolvedValue);
            }
        }
        return result;
    }

    @Override
    public boolean containsKey(String key) {
        checkClosed();
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(resolveKey(key));
        }
    }

    @Override
    public void loadAll(Set<? extends String> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        checkClosed();
        Thread t = new Thread(() -> loadAllRun(keys, replaceExistingValues, completionListener));
        t.setDaemon(true);
        t.start();
    }

    private void loadAllRun(Set<? extends String> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        try {
            loadAllNow(keys, replaceExistingValues);
            completionListener.onCompletion();
        } catch (Exception e) {
            completionListener.onException(e);
        }
    }

//    public void loadAllNow1(Set<? extends String> keys, boolean replaceExistingValues) {
//        checkClosed();
//        if (cacheLoader != null) {
//            for(String key: keys) {
//                if (replaceExistingValues || !containsKey(key)) {
//                    String value = cacheLoader.load(key);
//                    if (value != null) {
//                        put(key, value);
//                    }
//                }
//            }
//        }
//    }
//
//
//    public void loadAllNow2(Set<? extends String> keys, boolean replaceExistingValues) {
//        checkClosed();
//        if (cacheLoader != null) {
//            Map<String, String> newKeyValues = new HashMap<>();
//            for(String key: keys) {
//                if (replaceExistingValues || !containsKey(key)) {
//                    String value = cacheLoader.load(key);
//                    if (value != null) {
//                        newKeyValues.put(key, value);
//                    }
//                }
//            }
//            putAll(newKeyValues);
//        }
//    }

    public void loadAllNow(Set<? extends String> keys, boolean replaceExistingValues) {
        checkClosed();
        if (cacheLoader != null) {
            if (!replaceExistingValues) {
                keys = keys.stream().
                        filter(k -> !containsKey(k)).
                        collect(Collectors.toSet());
            }
            Map<String, String> newKeyValues = cacheLoader.loadAll(keys);
            putAll(newKeyValues);
        }
    }




    @Override
    public void put(String key, String value) {
        checkClosed();
        if (key == null) throw new NullPointerException("RedisCache.put key is null");
        if (value == null) throw new NullPointerException("RedisCache.put value is null");
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(resolveKey(key), value);
            if (cacheWriter != null) {
                cacheWriter.write(new RedisCacheEntry(key, value));
            }
        }
    }

    @Override
    public String getAndPut(String key, String value) {
        checkClosed();
        if (key == null) throw new NullPointerException("RedisCache.getAndPut key is null");
        if (value == null) throw new NullPointerException("RedisCache.getAndPut value is null");
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction t = jedis.multi();
            Response<String> response = t.get(resolveKey(key));
            t.set(resolveKey(key), value);
            t.exec();
            if (cacheWriter != null) {
                cacheWriter.write(new org.obapanel.jedis.cache.javaxcache.RedisCacheEntry(key, value));
            }
            return response.get();
        }
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> map) {
        checkClosed();
        if (map == null) throw new NullPointerException("RedisCache.putAll map is null");
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction t = jedis.multi();
            map.forEach( (k,v) -> t.set(resolveKey(k),v));
            t.exec();
            if (cacheWriter != null) {
                Collection<Cache.Entry<? extends String,? extends String>> entries = map.entrySet().
                        stream().
                        map(e -> new org.obapanel.jedis.cache.javaxcache.RedisCacheEntry(e.getKey(), e.getValue())).
                        collect(Collectors.toSet());
                cacheWriter.writeAll(entries);
            }
        }
    }


    @Override
    public boolean putIfAbsent(String key, String value) {
        checkClosed();
        if (key == null) throw new NullPointerException("RedisCache.putIfAbsent key is null");
        if (value == null) throw new NullPointerException("RedisCache.putIfAbsent value is null");
        try (Jedis jedis = jedisPool.getResource()) {
            String result = jedis.set(resolveKey(key), value, new SetParams().nx());
            if (result!= null && cacheWriter != null) {
                cacheWriter.write(new org.obapanel.jedis.cache.javaxcache.RedisCacheEntry(key, value));
            }
            return result != null;
        }
    }

    @Override
    public boolean remove(String key) {
        checkClosed();
        if (key == null) throw new NullPointerException("RedisCache.remove key is null");
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction t = jedis.multi();
            Response<String> previous = t.get(resolveKey(key));
            t.del(resolveKey(key));
            t.exec();
            if (previous.get() != null && cacheWriter != null) {
                cacheWriter.delete(key);
            }
            return previous.get() != null;
        }
    }

    @Override
    public boolean remove(String key, String oldValue) {
        checkClosed();
        //Better with script
        if (key == null) throw new NullPointerException("RedisCache.remove key is null");
        if (oldValue == null) throw new NullPointerException("RedisCache.remove oldValue is null");
        try (Jedis jedis = jedisPool.getResource()) {
            String current = jedis.get(resolveKey(key));
            if (current != null && current.equals(oldValue)) {
                jedis.del(resolveKey(key));
                if (cacheWriter != null) {
                    cacheWriter.delete(key);
                }
                return true;
            }  else {
                return false;
            }
        }
    }

    @Override
    public String getAndRemove(String key) {
        checkClosed();
        if (key == null) throw new NullPointerException("RedisCache.getAndRemove key is null");
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction t = jedis.multi();
            Response<String> previous = t.get(resolveKey(key));
            t.del(resolveKey(key));
            t.exec();
            if (previous.get() != null && cacheWriter != null) {
                cacheWriter.delete(key);
            }
            return previous.get();
        }
    }

    @Override
    public boolean replace(String key, String oldValue, String newValue) {
        checkClosed();
        if (key == null) throw new NullPointerException("RedisCache.replace key is null");
        if (oldValue == null) throw new NullPointerException("RedisCache.replace oldValue is null");
        if (newValue == null) throw new NullPointerException("RedisCache.replace newValue is null");

        //Better with script
        try (Jedis jedis = jedisPool.getResource()) {
            String current = jedis.get(resolveKey(key));
            if (current != null && current.equals(oldValue)) {
                jedis.set(resolveKey(key), newValue);
                if (cacheWriter != null) {
                    cacheWriter.write(new org.obapanel.jedis.cache.javaxcache.RedisCacheEntry(key, newValue));
                }
                return true;
            }  else {
                return false;
            }
        }
    }

    @Override
    public boolean replace(String key, String value) {
        checkClosed();
        if (key == null) throw new NullPointerException("RedisCache.replace key is null");
        if (value == null) throw new NullPointerException("RedisCache.replace value is null");
        //Better with script
        try (Jedis jedis = jedisPool.getResource()) {
            String current = jedis.get(resolveKey(key));
            if (current != null) {
                jedis.set(resolveKey(key), value);
                if (cacheWriter != null) {
                    cacheWriter.write(new org.obapanel.jedis.cache.javaxcache.RedisCacheEntry(key, value));
                }
                return true;
            }  else {
                return false;
            }
        }
    }

    @Override
    public String getAndReplace(String key, String value) {
        checkClosed();
        if (key == null) throw new NullPointerException("RedisCache.getAndReplace key is null");
        if (value == null) throw new NullPointerException("RedisCache.getAndReplace value is null");
        //Better with script
        try (Jedis jedis = jedisPool.getResource()) {
            String current = jedis.get(resolveKey(key));
            if (current != null) {
                jedis.set(resolveKey(key), value);
                if (cacheWriter != null) {
                    cacheWriter.write(new org.obapanel.jedis.cache.javaxcache.RedisCacheEntry(key, value));
                }
                return current;
            }  else {
                return null;
            }
        }
    }

    @Override
    public void removeAll(Set<? extends String> keys) {
        checkClosed();
        if (keys == null) throw new NullPointerException("RedisCache.removeAll keys is null");
        String[] keysAsArray = keys.toArray(new String[0]);
        for(int i=0; i < keysAsArray.length; i++) {
            keysAsArray[i] = resolveKey(keysAsArray[i]);
        }
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(keysAsArray);
        }
        if (cacheWriter != null) {
            cacheWriter.deleteAll(keys);
        }
    }

    @Override
    public void removeAll() {
        removeAll(true);
    }


    private void removeAll(boolean allowCacheWriter) {
        checkClosed();
        List<String> scanned =  ScanUtil.retrieveListOfKeys(jedisPool, resolveKey("*"));
        // No need to convert here
        if (!scanned.isEmpty()) {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.del(scanned.toArray(new String[]{}));
            }
            if (allowCacheWriter && cacheWriter != null) {
                List<String> unresolved = scanned.stream().
                        map(this::unresolveKey).
                        collect(Collectors.toList());
                cacheWriter.deleteAll(unresolved);
            }
        }
    }

    @Override
    public void clear() {
        removeAll(false);
    }

    @Override
    public <C extends Configuration<String, String>> C getConfiguration(Class<C> clazz) {
        return (C) configuration;
    }

    @Override
    public <T> T invoke(String key, EntryProcessor<String, String, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        checkClosed();
        RedisCacheInvokeEntry entry = new RedisCacheInvokeEntry(this, key);
        return entryProcessor.process(entry, arguments);
    }

    @Override
    public <T> Map<String, EntryProcessorResult<T>> invokeAll(Set<? extends String> keys, EntryProcessor<String, String, T> entryProcessor, Object... arguments) {
        Map<String, EntryProcessorResult<T>> results = new HashMap<>();
        for(String key: keys) {
            try {
                T result = invoke(key, entryProcessor, arguments);
                results.put(key, new RedisCacheInvokeEntryProcessorResult<>(result));
            } catch (Exception exception) {
                results.put(key, new RedisCacheInvokeEntryProcessorResult<>(exception));
            }
        }
        return results;
    }


    @Override
    public void close() {
        clear();
        isClosed.set(true);
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    private void checkClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("RedisCachingProvider is closed");
        }
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return RedisCacheUtils.unwrap(clazz, this);
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<String, String> cacheEntryListenerConfiguration) {

    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<String, String> cacheEntryListenerConfiguration) {

    }

    @Override
    public Iterator<Entry<String, String>> iterator() {
        checkClosed();
        return new RedisCacheIterator(this, jedisPool);
    }

}
