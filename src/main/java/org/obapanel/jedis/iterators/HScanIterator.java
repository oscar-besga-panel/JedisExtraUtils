package org.obapanel.jedis.iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

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
public class HScanIterator extends AbstractScanIterator<Map.Entry<String, String>> {


    private static final Logger LOGGER = LoggerFactory.getLogger(HScanIterator.class);

    private final String name;

    /**
     * Iterator for hmap entries
     * @param jedisPool Jedis connection pool
     * @param name Name of the hmap
     */
    public HScanIterator(JedisPool jedisPool, String name) {
        this(jedisPool, name, DEFAULT_PATTERN_ITERATORS, DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterator for hmap entries
     * @param jedisPool Jedis connection pool
     * @param name Name of the hmap
     * @param pattern Pattern to be matched on the responses
     */
    public HScanIterator(JedisPool jedisPool, String name, String pattern) {
        this(jedisPool, name, pattern, DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterator for hmap entries
     * @param jedisPool Jedis connection pool
     * @param name Name of the hmap
     * @param resultsPerScan results per call to redis
     */
    public HScanIterator(JedisPool jedisPool, String name, int resultsPerScan) {
        this(jedisPool, name, DEFAULT_PATTERN_ITERATORS, resultsPerScan);
    }

    /**
     * Iterator for hmap entries
     * @param jedisPool Jedis connection pool
     * @param name Name of the hmap
     * @param pattern Pattern to be matched on the responses
     * @param resultsPerScan results per call to redis
     */
    public HScanIterator(JedisPool jedisPool, String name, String pattern, int resultsPerScan) {
        super(jedisPool, pattern, resultsPerScan);
        this.name = name;
    }



    @Override
    ScanResult<Map.Entry<String, String>> doScan(Jedis jedis, String currentCursor, ScanParams scanParams) {
        return jedis.hscan(name, currentCursor, scanParams);
    }

    @Override
    void doRemove(Jedis jedis, Map.Entry<String, String> next) {
        jedis.hdel(name, next.getKey());
    }


}
