package org.oba.jedis.extra.utils.cache;

import java.util.HashMap;
import java.util.Map;

/**
 * Used to give a cache read-through capabilities or
 * when loading data into a cache from external sources (like a database)
 */
public interface CacheLoader {

    /**
     * Loads an object. Application developers should implement this
     * method to customize the loading of a value for a cache entry. This method
     * is called by a cache when a requested entry is not in the cache. If
     * the object can't be loaded <code>null</code> should be returned.
     *
     * @param key identifying the value to be loaded
     * @return value from external sources
     */
    String load(String key);

    /**
     * Loads multiple objects. Application developers should implement this
     * method to customize the loading of cache entries. This method is called
     * when the requested object is not in the cache. If an object can't be loaded,
     * it is not returned in the resulting map.
     *
     * A default, looping, non-optimized implementation is provided
     *
     * @param keys keys identifying the values to be loaded
     * @return A map of key, values to be stored in the cache.
     */
    default Map<String, String> loadAll(Iterable<String> keys) {
        final Map<String, String> results = new HashMap<>();
        keys.forEach( key -> {
            String value = load(key);
            if (value != null) {
                results.put(key, value);
            }
        });
        return results;
    }

}
