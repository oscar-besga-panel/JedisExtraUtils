package org.obapanel.jedis.cache.simple;

import org.obapanel.jedis.utils.Listable;
import org.obapanel.jedis.utils.Mapeable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.params.SetParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * A cache is a Map-like data structure that provides temporary storage
 * of application data.
 * This cache stores all his data in a Redis server, and all the operations
 * are done remotely on redis.
 * NO cache data is stored inside the class
 *
 * Like maps, a cache store key-value pairs and its iterable
 *
 * Keys inside redis are resolved with the cache name to avoid duplication in redis space
 *
 * Unlike maps, null keys or values aren't alllowed.
 * Every value has a timeout before it's deleted automatically by redis
 *
 * You can use a CacheLoader to give readthrough caching, that is
 * the ability to search externally for a value not present in the cache
 *
 * You can use a CacheWriter to give writethrough caching, that is
 * the ability to insert/update or delete a value when it's modified in the cache
 *
 * This cache works like a javax.cache.Cache
 * but simpler and less options
 * (no factories, events, mxbeans, statistics included)
 *
 * The cache must have a Jedis connection pool
 * Also a name, every instance with the same name will access the same redis data
 * And a timeout that will be applied to all data by default
 */
public class SimpleCache  implements Iterable<Map.Entry<String,String>>, Listable<Map.Entry<String,String>>, Mapeable<String, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleCache.class);


    private final JedisPool jedisPool;

    private final String name;

    private final long timeOutMs;

    private CacheLoader cacheLoader;

    private CacheWriter cacheWriter;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);


    /**
     * Create a simple cache
     * @param jedisPool Connection pool
     * @param name Name shared across instances
     * @param timeOutMs Default timeout for every entry
     */
    public SimpleCache(JedisPool jedisPool, String name, long timeOutMs) {
        this(jedisPool, name, timeOutMs, null);
    }

    /**
     * Create a simple cache
     * @param jedisPool Connection pool
     * @param name Name shared across instances
     * @param timeOutMs Default timeout for every entry
     * @param cacheLoader Default cacheloader readthrougth
     */
    public SimpleCache(JedisPool jedisPool, String name, long timeOutMs, CacheLoader cacheLoader) {
        this.jedisPool = jedisPool;
        this.name = name;
        this.timeOutMs = timeOutMs;
        this.cacheLoader = cacheLoader;
    }


    /**
     * Adds a cache loader to this cache
     * @param cacheLoader Default cacheloader readthrougth
     * @return cache
     */
    public SimpleCache withCacheLoader(CacheLoader cacheLoader) {
        this.cacheLoader = cacheLoader;
        return this;
    }

    /**
     * Adds a cache writer to this cache
     * @param cacheWriter Default cachewriter writethrough
     * @return cache
     */
    public SimpleCache withCacheWriter(CacheWriter cacheWriter) {
        this.cacheWriter = cacheWriter;
        return this;
    }

    /**
     * Return the current jedis pol
     * @return jedis pool
     */
    JedisPool getJedisPool() {
        return jedisPool;
    }

    /**
     * Current name
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Converts the key from external representation to internal (redis) one
     * No null accepted
     * @param key key to convert
     * @return key converted
     */
    public String resolveKey(String key) {
        if (key == null) throw new NullPointerException("Key must be not null");
        return name + ":" + key;
    }

    /**
     * Converts the key from internal (redis) representation to external one
     * No null accepted
     * @param key key to un-convert
     * @return key un-converted
     */
    public String unresolveKey(String key) {
        if (key == null) throw new NullPointerException("Key must be not null");
        return key.replace(name + ":", "");
    }

    /**
     * Gets current value from redis cache
     * If not found, it can use the default cacheloader readthrough if present
     * @param key not null key
     * @return value from cache or loaded, or null if no exists
     */
    public String get(String key) {
        return get(key, cacheLoader);
    }

    /**
     * Gets current value from redis cache
     * If not founnd, it can use the provided cacheloader readthrough
     * The default cacheloader is overriden
     * @param key not null key
     * @return value from cache or loaded, or null if no exists
     */
    public String get(String key, CacheLoader cacheLoader) {
        checkClosed();
        if (key == null) throw new NullPointerException("RedisCache.get key is null");
        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.get(resolveKey(key));
            if (value == null) {
                value = readThrougth(jedis, key, cacheLoader);
            }
            return value;
        }
    }

    /**
     * Gets values from redis cache
     * If not found, it can use the default cacheloader readthrough if present
     * Not found values will not be present in result map (null resutls are no retunred)
     * @param keys not null set of keys
     * @return map with values from cache or loaded, (no nulls)
     */
    public Map<String, String> getAll(Set<String> keys) {
        return getAll(keys, cacheLoader);
    }

    /**
     * Gets values from redis cache
     * If not found, it can use the given cacheloader readthrough
     * The default cacheloader is overriden
     * Not found values will not be present in result map (null resutls are no retunred)
     * @param keys not null set of keys
     * @return map with values from cache or loaded, (no nulls)
     */
    public Map<String, String> getAll(Set<String> keys, CacheLoader cacheLoader) {
        checkClosed();
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, Response<String>> responses = new HashMap<>();
            Transaction t = jedis.multi();
            for(String key: keys) {
                responses.put(key, t.get(resolveKey(key)));
            }
            t.exec();
            return resolveTransactionEntries(jedis, responses, cacheLoader);
        }
    }

    /**
     * Covert data from trasn
     * @param jedis connection
     * @param responses Map of key reponsese
     * @param cacheLoader current cache loader of the operation
     * @return map with key values from cache
     */
    private Map<String, String> resolveTransactionEntries(Jedis jedis, Map<String, Response<String>> responses, CacheLoader cacheLoader) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, Response<String>> entry: responses.entrySet()) {
            String resolvedValue;
            if (entry.getValue() != null && entry.getValue().get() != null) {
                resolvedValue = entry.getValue().get();
            } else {
                //TODO use a loadAll ?
                resolvedValue = readThrougth(jedis, entry.getKey(), cacheLoader);
            }
            if (resolvedValue != null) {
                result.put(entry.getKey(), resolvedValue);
            }
        }
        return result;
    }

    /**
     * Gets a value from the external source,
     * and updates it in jedis if not null
     * @param jedis  Jedis connnection
     * @param key Key
     * @param cacheLoader Current cache loader of operation
     * @return external value, null if not exists
     */
    private String readThrougth(Jedis jedis, String key, CacheLoader cacheLoader) {
        String value = null;
        if (cacheLoader != null) {
            LOGGER.debug("readThrougth key {}", key);
            value = cacheLoader.load(key);
            if (value != null) {
                jedis.set(resolveKey(key), value);
            }
        }
        return value;
    }

    /**
     * Checks if a key exists in redis
     * @param key Key
     * @return true if a value is bound to this key
     */
    public boolean containsKey(String key) {
        checkClosed();
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(resolveKey(key));
        }
    }

    /**
     * Load the set of keys from external sources, updating their values in redis
     * If no cacheLoader exists, it does nothing
     * @param keys Set of keys to update
     * @param replaceExistingValues if a value is found in redis, should it be updated
     */
    public void loadAll(Set<String> keys, boolean replaceExistingValues) {
        checkClosed();
        if (cacheLoader != null) {
            if (!replaceExistingValues) {
                keys = keys.stream().
                        filter(k -> !containsKey(k)).
                        collect(Collectors.toSet());
            }
            LOGGER.debug("readThrougth keys {}", keys);
            Map<String, String> newKeyValues = cacheLoader.loadAll(keys);
            putAll(newKeyValues, false);
        }
    }

    /**
     * Inserts a new value for given key in redis
     * The new value will use the default timeout of the cache
     * If the value exists, it will be overwritten
     * If a cacheWriter is present, it will be updated in external sources by writethrougth
     * @param key Key of the value
     * @param value Data of the value
     */
    public void put(String key, String value) {
        put(key, value, timeOutMs);
    }

    /**
     * Inserts a new value for given key in redis
     * The new value will use given timeout
     * If the value exists, it will be overwritten
     * If a cacheWriter is present, it will be updated in external sources by writethrougth
     * @param key Key of the value
     * @param value Data of the value
     * @param timeOutMs Time to live
     */
    public void put(String key, String value, long timeOutMs) {
        checkClosed();
        if (key == null) throw new NullPointerException("RedisCache.put key is null");
        if (value == null) throw new NullPointerException("RedisCache.put value is null");
        try (Jedis jedis = jedisPool.getResource()) {
            SetParams setParams = new SetParams().px(timeOutMs);
            jedis.set(resolveKey(key), value, setParams);
            if (cacheWriter != null) {
                LOGGER.debug("writeThrougth key {} value {}", key, value);
                cacheWriter.write(key, value);
            }
        }
    }

    /**
     * Inserts a new value for given key in redis
     * The new value will use the default timeout of the cache
     * If the value exists it will be returned in this method (and overwritten)
     * If a cacheWriter is present, it will be updated in external sources by writethrougth
     * @param key Key of the value
     * @param value Data of the value
     * @return previous value, null if there was no one
     */
    public String getAndPut(String key, String value) {
        checkClosed();
        if (key == null) throw new NullPointerException("RedisCache.getAndPut key is null");
        if (value == null) throw new NullPointerException("RedisCache.getAndPut value is null");
        try (Jedis jedis = jedisPool.getResource()) {
            SetParams setParams = new SetParams().px(timeOutMs);
            Transaction t = jedis.multi();
            Response<String> response = t.get(resolveKey(key));
            t.set(resolveKey(key), value, setParams);
            t.exec();
            if (cacheWriter != null) {
                LOGGER.debug("writeThrougth key {} value {}", key, value);
                cacheWriter.write(key, value);
            }
            return response.get();
        }
    }

    /**
     * Inserts a list of new key-values in cache
     * The new values will use the default timeout of the cache
     * If any value exists, it will be overwritten
     * If a cacheWriter is present, it will be updated in external sources by writethrougth
     * @param values map of the key-values data
     */
    public void putAll(Map<String,String> values) {
        putAll(values, true);
    }

    /**
     * Inserts a list of new key-values in cache
     * The new values will use the default timeout of the cache
     * If any value exists, it will be overwritten
     * If a cacheWriter is present and the second parameter is true,
     * it will be updated in external sources by writethrougth
     * @param values map of the key-values data
     * @param allowWriteThrougth if the values should be updated in external datasource
     */
    private void putAll(Map<String,String> values, boolean allowWriteThrougth) {
        checkClosed();
        if (values == null) throw new NullPointerException("RedisCache.putAll map is null");
        try (Jedis jedis = jedisPool.getResource()) {
            SetParams setParams = new SetParams().px(timeOutMs);
            Transaction t = jedis.multi();
            values.forEach( (k,v) -> t.set(resolveKey(k),v, setParams));
            t.exec();
            if (allowWriteThrougth && cacheWriter != null) {
                LOGGER.debug("writeThrougth values {}", values);
                cacheWriter.writeAll(values);
            }
        }
    }

    /**
     * Inserts a new value for given key in redis if no previous value is present
     * Nothing will be done otherwise
     * The new value will use given timeout
     * If a cacheWriter is present and can be updated in redis,
     * it will be updated in external sources by writethrougth
     * @param key Key of the value
     * @param value Data of the value
     */
    public boolean putIfAbsent(String key, String value) {
        checkClosed();
        if (key == null) throw new NullPointerException("RedisCache.putIfAbsent key is null");
        if (value == null) throw new NullPointerException("RedisCache.putIfAbsent value is null");
        try (Jedis jedis = jedisPool.getResource()) {
            SetParams setParams = new SetParams().nx().px(timeOutMs);
            String result = jedis.set(resolveKey(key), value, setParams );
            if (result!= null && cacheWriter != null) {
                LOGGER.debug("writeThrougth key {} value {}", key, value);
                cacheWriter.write(key, value);
            }
            return result != null;
        }
    }

    /**
     * Removes for given key in redis
     * If the value doesn't exist, nothing will happen
     * If a cacheWriter is present, it will be removed in external sources by writethrougth
     * @param key Key of the value
     * @return true if a values has been removed
     */
    public boolean remove(String key) {
        checkClosed();
        if (key == null) throw new NullPointerException("RedisCache.remove key is null");
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction t = jedis.multi();
            Response<String> previous = t.get(resolveKey(key));
            t.del(resolveKey(key));
            t.exec();
            if (previous.get() != null && cacheWriter != null) {
                LOGGER.debug("deleteThrougth key {} ", key);
                cacheWriter.delete(key);
            }
            return previous.get() != null;
        }
    }

    /**
     * Removes for given key in redis if the value matches
     * The deletion will happen if current value matches withi given value
     * If a cacheWriter is present and the deletion is done,
     * it will be removed in external sources by writethrougth
     * @param key Key of the value
     * @param oldValue value that must equal to redis one to execute deletion
     * @return true if deleted
     */
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
                    LOGGER.debug("deleteThrougth key {} ", key);
                    cacheWriter.delete(key);
                }
                return true;
            }  else {
                return false;
            }
        }
    }

    /**
     * Removes for given key in redis and returns the current redis value
     *
     * If a cacheWriter is present,
     * it will be removed in external sources by writethrougth
     * @param key Key of the value
     * @return previous value or null if ther wasn't one
     */
    public String getAndRemove(String key) {
        checkClosed();
        if (key == null) throw new NullPointerException("RedisCache.getAndRemove key is null");
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction t = jedis.multi();
            Response<String> previous = t.get(resolveKey(key));
            t.del(resolveKey(key));
            t.exec();
            if (previous.get() != null && cacheWriter != null) {
                LOGGER.debug("deleteThrougth key {} ", key);
                cacheWriter.delete(key);
            }
            return previous.get();
        }
    }

    /**
     * Replaces key value with new value if current redis value is equals to given oldValue
     * If value is replaced and a cachewriter exits, external source is updated too writethrougth
     * @param key Key to have replacement
     * @param oldValue Value to be matched
     * @param newValue New value to update in redis
     * @return true if replaced
     */
    public boolean replace(String key, String oldValue, String newValue) {
        checkClosed();
        if (key == null) throw new NullPointerException("RedisCache.replace key is null");
        if (oldValue == null) throw new NullPointerException("RedisCache.replace oldValue is null");
        if (newValue == null) throw new NullPointerException("RedisCache.replace newValue is null");

        //Better with script
        try (Jedis jedis = jedisPool.getResource()) {
            String current = jedis.get(resolveKey(key));
            if (current != null && current.equals(oldValue)) {
                SetParams setParams = new SetParams().px(timeOutMs);
                jedis.set(resolveKey(key), newValue, setParams);
                if (cacheWriter != null) {
                    LOGGER.debug("writeThrougth key {} value {}", key, newValue);
                    cacheWriter.write(key, newValue);
                }
                return true;
            }  else {
                return false;
            }
        }
    }

    /**
     * Replaces key value with new value if key exists
     * If value is replaced and a cachewriter exits, external source is updated too writethrougth
     * @param key Key to have replacement
     * @param value New value to update in redis
     * @return true if replaced
     */
    public boolean replace(String key, String value) {
        checkClosed();
        if (key == null) throw new NullPointerException("RedisCache.replace key is null");
        if (value == null) throw new NullPointerException("RedisCache.replace value is null");
        //Better with script
        try (Jedis jedis = jedisPool.getResource()) {
            String current = jedis.get(resolveKey(key));
            if (current != null) {
                SetParams setParams = new SetParams().px(timeOutMs);
                jedis.set(resolveKey(key), value, setParams);
                if (cacheWriter != null) {
                    LOGGER.debug("writeThrougth key {} value {}", key, value);
                    cacheWriter.write(key, value);
                }
                return true;
            }  else {
                return false;
            }
        }
    }

    /**
     * Will return the current value for a specified key, and replace it with the given new value
     * If not value is present in redis cache, no operation will be done
     * This is equivalent to
     *   if cache.containsKey(key) {
     *     V oldValue = cache.get(key)
     *     cache.put(key, value)
     *     return oldValue
     *   } else {
     *     return null
     *   }
     *
     * writethrough -> If the value is going to be updated in redis, in external system too
     * @param key Key to be modified
     * @param value New value to be updated
     * @return Old value in cache
     */
    public String getAndReplace(String key, String value) {
        checkClosed();
        if (key == null) throw new NullPointerException("RedisCache.getAndReplace key is null");
        if (value == null) throw new NullPointerException("RedisCache.getAndReplace value is null");
        //Better with script
        try (Jedis jedis = jedisPool.getResource()) {
            String current = jedis.get(resolveKey(key));
            if (current != null) {
                SetParams setParams = new SetParams().px(timeOutMs);
                jedis.set(resolveKey(key), value, setParams);
                if (cacheWriter != null) {
                    LOGGER.debug("writeThrougth key {} value {}", key, value);
                    cacheWriter.write(key, value);
                }
                return current;
            }  else {
                return null;
            }
        }
    }

    /**
     * Will remove entries from redis with the given keys
     * writethrough -> If a cacheWriter is present, all external values wil be deleted
     * @param keys keys to remove
     */
    public void removeAll(Set<String> keys) {
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
            LOGGER.debug("deleteThrougth keys {} ", keys);
            cacheWriter.deleteAll(keys);
        }
    }

    /**
     * Will remove ALL entries from redis
     * It will not affect external values
     */
    public void clear() {
        removeAll(false);
    }

    /**
     * Will remove ALL entries from redis
     * writethrough -> If a cacheWriter is present, all external values wil be deleted
     */
    public void removeAll() {
        removeAll(true);
    }


    /**
     * Will remove ALL entries from redis
     * @param allowCacheWriter use cacheWriter if present
     */
    private void removeAll(boolean allowCacheWriter) {
        checkClosed();
        List<String> scanned = new CacheKeyIterator(this, false).asList();
        // No need to convert here
        if (!scanned.isEmpty()) {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.del(scanned.toArray(new String[]{}));
            }
            if (allowCacheWriter && cacheWriter != null) {
                List<String> unresolved = scanned.stream().
                        map(this::unresolveKey).
                        collect(Collectors.toList());
                LOGGER.debug("deleteThrougth key {} ", unresolved);
                cacheWriter.deleteAll(unresolved);
            }
        }
    }

    /**
     * Return an iterator for current key-value pairs
     * Until hasNext / next is called, no data is retrieved from redis
     * @return CacheKeyIterator
     */
    public CacheIterator iterator() {
        checkClosed();
        return new CacheIterator(this);
    }

    /**
     * Return an iterator for current keys
     * Until hasNext / next is called, no data is retrieved from redis
     * @return CacheKeyIterator
     */
    public CacheKeyIterator keysIterator() {
        checkClosed();
        return new CacheKeyIterator(this);
    }

    /**
     * Return all the current keys in the cache into a local unmodifiable list
     * @return list with keys
     */
    public List<String> keys() {
        checkClosed();
        return keysIterator().asList();
    }

    /**
     * Return all the current key-value pairs in the cache into a local unmodifiable entry list
     * This will make N access to redis and make a copy of the remote data
     * @return list with data
     */
    @Override
    public List<Map.Entry<String, String>> asList() {
        return iterator().asList();
    }

    /**
     * Return all the current key-value pairs in the cache into a local unmodifiable map
     * This will make N access to redis and make a copy of the remote data
     * @return map with data
     */
    public Map<String, String> asMap() {
        return iterator().asMap();
    }


    /**
     * Close this instance of redis cache
     * Does not affect redis of extenal data
     */
    public void close() {
        isClosed.set(true);
    }

    /**
     * Check if closed
     * @return true if closed
     */
    public boolean isClosed() {
        return isClosed.get();
    }

    /**
     * If cache is closed, an exception will abort any operation
     */
    void checkClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("RedisCachingProvider is closed");
        }
    }

}
