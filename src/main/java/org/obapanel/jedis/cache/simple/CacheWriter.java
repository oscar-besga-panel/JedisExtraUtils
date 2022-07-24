package org.obapanel.jedis.cache.simple;

import java.util.Collection;
import java.util.Map;

/**
 * Used to give a cache write-through capabilities or
 * when storing data into a cache, to also store in external sources (like a database)
 */
public interface CacheWriter {


    /**
     * Write the specified value under the specified key to the external resource.
     *
     * This method is intended to support both key/value creation and value update
     * for a specific key.
     *
     * @param entry the entry to be written
     */
    default void write(Map.Entry<String, String> entry) {
        write(entry.getKey(), entry.getValue());
    }


    /**
     * Write the specified value under the specified key to the external resource.
     *
     * This method is intended to support both key/value creation and value update
     * for a specific key.
     *
     * @param key the key to be written
     * @param value the value to be written
     */
    void write(String key, String value);

    /**
     * Write the specified entries to the external resource. This method is intended
     * to support both insert and update.
     *
     * A default, looping, non-optimized implementation is provided
     *
     * @param values a collection to write.
     */
    default void writeAll(Map<String, String> values) {
        values.forEach(this::write);
    }


    /**
     * Delete the cache entry from the external resource.
     *
     * @param key the key that is used for the delete operation
     */
    void delete(String key);


    /**
     * Remove data and keys from the external resource for the given collection of
     * keys, if present.
     *
     * A default, looping, non-optimized implementation is provided
     *
     * @param keys a collection of keys for entries to delete.
     */
    default void deleteAll(Collection<String> keys) {
        keys.forEach(this::delete);
    }

}
