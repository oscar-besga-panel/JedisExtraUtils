package org.oba.jedis.extra.utils.iterators;

import org.oba.jedis.extra.utils.utils.Named;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

/**
 * Iterator for sset entries
 * Only one use for an instace of this class
 *
 * Jedis pool connection is required
 * Name of the element on redis is required
 * (if not exists, it acts like as called for an empty set)
 *
 * If no pattern is provided, all elements are retrieves interactively
 * If no results per call to redis, it tries with 1
 *
 * Can return duplicated results, but is rare
 */
public class SScanIterator extends AbstractScanIterator<String> implements  Named {

    private final String name;

    /**
     * Iterator for sset entries
     * @param jedisPool Jedis connection pool
     * @param name Name of the set
     */
    public SScanIterator(JedisPool jedisPool, String name) {
        this(jedisPool, name, DEFAULT_PATTERN_ITERATORS, DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterator for sset entries
     * @param jedisPool Jedis connection pool
     * @param name Name of the set
     * @param pattern Pattern to be matched on the responses
     */
    public SScanIterator(JedisPool jedisPool, String name, String pattern) {
        this(jedisPool, name, pattern, DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterator for sset entries
     * @param jedisPool Jedis connection pool
     * @param name Name of the set
     * @param resultsPerScan results per call to redis
     */
    public SScanIterator(JedisPool jedisPool, String name, int resultsPerScan) {
        this(jedisPool, name, DEFAULT_PATTERN_ITERATORS, resultsPerScan);
    }

    /**
     * Iterator for sset entries
     * @param jedisPool Jedis connection pool
     * @param name Name of the set
     * @param pattern Pattern to be matched on the responses
     * @param resultsPerScan results per call to redis
     */
    public SScanIterator(JedisPool jedisPool, String name, String pattern, int resultsPerScan) {
        super(jedisPool, pattern, resultsPerScan);
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    ScanResult<String> doScan(Jedis jedis, String currentCursor, ScanParams scanParams) {
        return jedis.sscan(name, currentCursor, scanParams);
    }

    @Override
    void doRemove(Jedis jedis, String next) {
        jedis.srem(name, next);
    }


}
