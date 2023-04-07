package org.oba.jedis.extra.utils.iterators;

import org.oba.jedis.extra.utils.utils.Listable;
import org.oba.jedis.extra.utils.utils.Mapeable;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Map;

/**
 * Iterable for hmap entries
 * Jedis pool connection is required
 * Name of the element on redis is required
 * (if not exists, it acts like as called for an empty map)
 *
 * If no pattern is provided, all elements are retrieves interactively
 * If no results per call to redis, it tries with 1
 *
 * Can return duplicated results, but is rare
 */
public class HScanIterable implements Iterable<Map.Entry<String,String>>,
        Listable<Map.Entry<String,String>>, Mapeable<String, String> {

    private final JedisPool jedisPool;
    private final String name;
    private final String pattern;
    private final int resultsPerScan;

    /**
     * Iterable for hmap entries
     * @param jedisPool Jedis connection pool
     * @param name Name of the hmap
     */
    public HScanIterable(JedisPool jedisPool, String name){
        this(jedisPool, name, AbstractScanIterator.DEFAULT_PATTERN_ITERATORS, AbstractScanIterator.DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterable for hmap entries
     * @param jedisPool Jedis connection pool
     * @param name Name of the hmap
     * @param pattern Pattern to be matched on the responses
     */
    public HScanIterable(JedisPool jedisPool, String name, String pattern){
        this(jedisPool, name, pattern, AbstractScanIterator.DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterable for hmap entries
     * @param jedisPool Jedis connection pool
     * @param name Name of the hmap
     * @param resultsPerScan results per call to redis
     */
    public HScanIterable(JedisPool jedisPool, String name, int resultsPerScan){
        this(jedisPool, name, AbstractScanIterator.DEFAULT_PATTERN_ITERATORS, resultsPerScan);
    }

    /**
     * Iterable for hmap entries
     * @param jedisPool Jedis connection pool
     * @param name Name of the hmap
     * @param pattern Pattern to be matched on the responses
     * @param resultsPerScan results per call to redis
     */
    public HScanIterable(JedisPool jedisPool, String name, String pattern, int resultsPerScan){
        this.jedisPool = jedisPool;
        this.name = name;
        this.pattern = pattern;
        this.resultsPerScan = resultsPerScan;
    }


    @Override
    public HScanIterator iterator() {
        return new HScanIterator(jedisPool, name, pattern, resultsPerScan);
    }

    @Override
    public List<Map.Entry<String,String>> asList() {
        return iterator().asList();
    }

    public Map<String, String> asMap() {
        return iterator().asMap();
    }

}
