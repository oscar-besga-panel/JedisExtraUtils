package org.oba.jedis.extra.utils.iterators;

import org.oba.jedis.extra.utils.utils.Mapeable;
import org.oba.jedis.extra.utils.utils.Named;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Iterator scan for the keys of a hmap
 * Only one use for an instace of this class
 *
 * Jedis pool connection is required
 * Name of the element on redis is required
 * (if not exists, it acts like as called for an empty map)
 *
 * If no pattern is provided, all elements are retrieves interactively
 * If no results per call to redis, it tries with 1
 *
 * Can return duplicated results, but is rare
 */
public class HScanIterator extends AbstractScanIterator<Map.Entry<String, String>>
        implements Mapeable<String, String>, Named {

    private final String name;

    /**
     * Iterator for hmap entries
     * @param jedisPooled Jedis connection pool
     * @param name Name of the hmap
     */
    public HScanIterator(JedisPooled jedisPooled, String name) {
        this(jedisPooled, name, DEFAULT_PATTERN_ITERATORS, DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterator for hmap entries
     * @param jedisPooled Jedis connection pool
     * @param name Name of the hmap
     * @param pattern Pattern to be matched on the responses
     */
    public HScanIterator(JedisPooled jedisPooled, String name, String pattern) {
        this(jedisPooled, name, pattern, DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterator for hmap entries
     * @param jedisPooled Jedis connection pool
     * @param name Name of the hmap
     * @param resultsPerScan results per call to redis
     */
    public HScanIterator(JedisPooled jedisPooled, String name, int resultsPerScan) {
        this(jedisPooled, name, DEFAULT_PATTERN_ITERATORS, resultsPerScan);
    }

    /**
     * Iterator for hmap entries
     * @param jedisPooled Jedis connection pool
     * @param name Name of the hmap
     * @param pattern Pattern to be matched on the responses
     * @param resultsPerScan results per call to redis
     */
    public HScanIterator(JedisPooled jedisPooled, String name, String pattern, int resultsPerScan) {
        super(jedisPooled, pattern, resultsPerScan);
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    ScanResult<Map.Entry<String, String>> doScan(JedisPooled jedisPooled, String currentCursor, ScanParams scanParams) {
        return jedisPooled.hscan(name, currentCursor, scanParams);
    }

    @Override
    void doRemove(JedisPooled jedisPooled, Map.Entry<String, String> next) {
        jedisPooled.hdel(name, next.getKey());
    }


    /**
     * This method returns ALL of the values of the iterator as a unmodificable map
     * This method consumes the iterator, no next and hasNetx method shoould be called
     * The map is unmodificable and contains no repeated elements
     * @return map with values
     */
    public Map<String, String> asMap(){
        final Map<String, String> map = new HashMap<>();
        forEachRemaining( e -> map.put(e.getKey(), e.getValue()));
        return Collections.unmodifiableMap(map);
    }

}
