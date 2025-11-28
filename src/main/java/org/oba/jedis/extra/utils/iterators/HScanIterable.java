package org.oba.jedis.extra.utils.iterators;

import org.oba.jedis.extra.utils.utils.Listable;
import org.oba.jedis.extra.utils.utils.Mapeable;
import org.oba.jedis.extra.utils.utils.Named;
import redis.clients.jedis.JedisPooled;

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
        Listable<Map.Entry<String,String>>, Mapeable<String, String>, Named {

    private final JedisPooled jedisPooled;
    private final String name;
    private final String pattern;
    private final int resultsPerScan;

    /**
     * Iterable for hmap entries
     * @param jedisPooled Jedis connection pool
     * @param name Name of the hmap
     */
    public HScanIterable(JedisPooled jedisPooled, String name){
        this(jedisPooled, name, AbstractScanIterator.DEFAULT_PATTERN_ITERATORS, AbstractScanIterator.DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterable for hmap entries
     * @param jedisPooled Jedis connection pool
     * @param name Name of the hmap
     * @param pattern Pattern to be matched on the responses
     */
    public HScanIterable(JedisPooled jedisPooled, String name, String pattern){
        this(jedisPooled, name, pattern, AbstractScanIterator.DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterable for hmap entries
     * @param jedisPooled Jedis connection pool
     * @param name Name of the hmap
     * @param resultsPerScan results per call to redis
     */
    public HScanIterable(JedisPooled jedisPooled, String name, int resultsPerScan){
        this(jedisPooled, name, AbstractScanIterator.DEFAULT_PATTERN_ITERATORS, resultsPerScan);
    }

    /**
     * Iterable for hmap entries
     * @param jedisPooled Jedis connection pool
     * @param name Name of the hmap
     * @param pattern Pattern to be matched on the responses
     * @param resultsPerScan results per call to redis
     */
    public HScanIterable(JedisPooled jedisPooled, String name, String pattern, int resultsPerScan){
        this.jedisPooled = jedisPooled;
        this.name = name;
        this.pattern = pattern;
        this.resultsPerScan = resultsPerScan;
    }


    @Override
    public String getName() {
        return name;
    }

    @Override
    public HScanIterator iterator() {
        return new HScanIterator(jedisPooled, name, pattern, resultsPerScan);
    }

    @Override
    public List<Map.Entry<String,String>> asList() {
        return iterator().asList();
    }

    @Override
    public Map<String, String> asMap() {
        return iterator().asMap();
    }

}
