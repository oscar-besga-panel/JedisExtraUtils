package org.obapanel.jedis.iterators;

import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;

import static org.obapanel.jedis.iterators.AbstractScanIterator.DEFAULT_PATTERN_ITERATORS;
import static org.obapanel.jedis.iterators.AbstractScanIterator.DEFAULT_RESULTS_PER_SCAN_ITERATORS;

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
public class HScanIterable implements Iterable<Map.Entry<String,String>> {

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
        this(jedisPool, name, DEFAULT_PATTERN_ITERATORS, DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterable for hmap entries
     * @param jedisPool Jedis connection pool
     * @param name Name of the hmap
     * @param pattern Pattern to be matched on the responses
     */
    public HScanIterable(JedisPool jedisPool, String name, String pattern){
        this(jedisPool, name, pattern, DEFAULT_RESULTS_PER_SCAN_ITERATORS);
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
    public void forEach(Consumer<? super Map.Entry<String,String>> action) {
        Iterable.super.forEach(action);
    }

    @Override
    public Spliterator<Map.Entry<String,String>> spliterator() {
        return Iterable.super.spliterator();
    }

}
