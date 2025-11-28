package org.oba.jedis.extra.utils.iterators;

import org.oba.jedis.extra.utils.utils.Named;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.Tuple;

/**
 * Iterator for zset entries
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
public class ZScanIterator extends AbstractScanIterator<Tuple> implements Named {


    private final String name;

    /**
     * Iterator for zset entries (ordered set)
     * @param jedisPooled Jedis connection pool
     * @param name Name of the set
     */
    public ZScanIterator(JedisPooled jedisPooled, String name) {
        this(jedisPooled, name, DEFAULT_PATTERN_ITERATORS, DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterator for zset entries (ordered set)
     * @param jedisPooled Jedis connection pool
     * @param name Name of the set
     * @param pattern Pattern to be matched on the responses
     */
    public ZScanIterator(JedisPooled jedisPooled, String name, String pattern) {
        this(jedisPooled, name, pattern, DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterator for zset entries (ordered set)
     * @param jedisPooled Jedis connection pool
     * @param name Name of the set
     * @param resultsPerScan results per call to redis
     */
    public ZScanIterator(JedisPooled jedisPooled, String name, int resultsPerScan) {
        this(jedisPooled, name, DEFAULT_PATTERN_ITERATORS, resultsPerScan);
    }

    /**
     * Iterator for zset entries (ordered set)
     * @param jedisPooled Jedis connection pool
     * @param name Name of the set
     * @param pattern Pattern to be matched on the responses
     * @param resultsPerScan results per call to redis
     */
    public ZScanIterator(JedisPooled jedisPooled, String name, String pattern, int resultsPerScan) {
        super(jedisPooled, pattern, resultsPerScan);
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }


    @Override
    ScanResult<Tuple> doScan(JedisPooled jedisPooled, String currentCursor, ScanParams scanParams) {
        return jedisPooled.zscan(name, currentCursor, scanParams);
    }

    @Override
    void doRemove(JedisPooled jedisPooled, Tuple next) {
        jedisPooled.zrem(name, next.getElement());
    }


}
