package org.obapanel.jedis.collections;

import org.obapanel.jedis.iterators.HScanIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class JedisMap implements Map<String, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisMap.class);

    private final JedisPool jedisPool;
    private final String name;

    /**
     * Creates a new list in jedis with given name, or references an existing one
     * This constructor doesn't affect the redis server
     * So really, creating here a list does not generate new data on Redis; the list on the server will
     *   exists when data is inserted
     * @param jedisPool Jedis pool connection
     * @param name Name of list on server
     */
    public JedisMap(JedisPool jedisPool, String name){
        this.jedisPool = jedisPool;
        this.name = name;
    }

    /**
     * Creates a new map in jedis with given name, or references an existing one
     * If the map doesn't exists, the 'from' data is stored,
     * if the map already exists, the 'from' data is added to the map
     * @param jedisPool Jedis pool connection
     * @param name Name of list on server
     * @param from Data to add to the map
     */
    public JedisMap(JedisPool jedisPool, String name, Map<String, String> from){
        this(jedisPool, name);
        putAll(from);
    }

    /**
     * Name of the redis map
     * @return redis name
     */
    public String getName() {
        return name;
    }


    /**
     * If map exist in Redis namespace
     * @return true if there is a reference in redis namespace, false otherwise
     */
    public boolean exists() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(name);
        }
    }

    /**
     * Checks if map exist in Redis namespace
     * @throws IllegalStateException if there is not a reference in redis namespace
     */
    public void checkExists() {
        if (!exists()) {
            throw new IllegalStateException("Current map  " + name + " not found in redis server");
        }
    }


    /**
     * Returns a map in java memory with the data of the map on redis
     * It copies the redis data in java process
     * (current implementation is an Hashmap, this may change)
     * @return map of data
     */
    //TODO test
    public Map<String, String> asMap(){
        Map<String, String> data = new HashMap<>();
        // I prefer to have more control here
        // data.putAll(this);
        entrySet().forEach( es -> data.put(es.getKey(), es.getValue()));
        return data;
    }

    @Override
    public int size() {
        try (Jedis jedis = jedisPool.getResource()) {
            long value = jedis.hlen(name);
            return Long.valueOf(value).intValue();
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hexists(name, (String) key);
        }
    }

    @Override
    public boolean containsValue(Object value) {
        // Could be lua optimized ?
        return values().contains(value);
    }

    @Override
    public String get(Object key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hget(name, (String) key);
        }
    }

    @Override
    public String put(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction t = jedis.multi();
            Response<String> previous = t.hget(name, key);
            t.hset(name, key, value);
            t.exec();
            return previous.get();
        }

    }

    @Override
    public String remove(Object key) {
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction t = jedis.multi();
            Response<String> previous = t.hget(name, (String) key);
            t.hdel(name, (String) key);
            t.exec();
            return previous.get();
        }
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        // Quick dirty way
//        for(Entry<? extends String, ? extends String> data : m.entrySet()){
//            put(data.getKey(), data.getValue());
//        }
        // better way
        try (Jedis jedis = jedisPool.getResource()) {
            final Transaction t = jedis.multi();
            for(Entry<? extends String, ? extends String> entry : m.entrySet()){
                t.hset(name, entry.getKey(), entry.getValue());
            }
//            m.entrySet().
//                    forEach( entry ->
//                            t.hset(name, entry.getKey(), entry.getValue())
//                    );
            t.exec();
        }
    }

    @Override
    public void clear() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(name);
        }
    }

    @Override
    public Set<String> keySet() {
        // I was thinking in using hkeys,
        // but I don't want to block the main thread
        Set<Entry<String, String>> keyValues = doHscan();
        return keyValues.stream().
                map(Entry::getKey).
                collect(Collectors.toSet());
    }

    @Override
    public Collection<String> values() {
        // I was thinking in using hvals,
        // but I don't want to block the main thread
        Set<Entry<String, String>> keyValues = doHscan();
        return keyValues.stream().
                map(Entry::getValue).
                collect(Collectors.toList());
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return doHscan();
    }

    private Set<Entry<String, String>> doHscan() {
        Set<Entry<String, String>> keyValues = new HashSet<>();
        HScanIterator hScanIterator = new HScanIterator(jedisPool, name);
        while (hScanIterator.hasNext()){
            keyValues.add(hScanIterator.next());
        }
        return keyValues;
    }

}
