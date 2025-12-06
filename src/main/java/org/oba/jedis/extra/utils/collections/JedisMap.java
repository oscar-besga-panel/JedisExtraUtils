package org.oba.jedis.extra.utils.collections;

import org.oba.jedis.extra.utils.iterators.HScanIterator;
import org.oba.jedis.extra.utils.utils.Named;
import redis.clients.jedis.AbstractTransaction;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Response;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class JedisMap implements Map<String, String>, Named {

    private final JedisPooled jedisPooled;
    private final String name;

    /**
     * Creates a new list in jedis with given name, or references an existing one
     * This constructor doesn't affect the redis server
     * So really, creating here a list does not generate new data on Redis; the list on the server will
     *   exists when data is inserted
     * @param jedisPooled Jedis pool connection
     * @param name Name of list on server
     */
    public JedisMap(JedisPooled jedisPooled, String name){
        this.jedisPooled = jedisPooled;
        this.name = name;
    }

    /**
     * Creates a new map in jedis with given name, or references an existing one
     * If the map doesn't exists, the 'from' data is stored,
     * if the map already exists, the 'from' data is added to the map
     * @param jedisPooled Jedis pool connection
     * @param name Name of list on server
     * @param from Data to add to the map
     */
    public JedisMap(JedisPooled jedisPooled, String name, Map<String, String> from){
        this(jedisPooled, name);
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
        return jedisPooled.exists(name);
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
        long value = jedisPooled.hlen(name);
        return Long.valueOf(value).intValue();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return jedisPooled.hexists(name, (String) key);
    }

    @Override
    public boolean containsValue(Object value) {
        // Could be lua optimized ?
        return values().contains(value);
    }

    @Override
    public String get(Object key) {
        return jedisPooled.hget(name, (String) key);
    }

    @Override
    public String put(String key, String value) {
        AbstractTransaction t = jedisPooled.multi();
        Response<String> previous = t.hget(name, key);
        t.hset(name, key, value);
        t.exec();
        return previous.get();
    }

    @Override
    public String remove(Object key) {
        AbstractTransaction t = jedisPooled.multi();
        Response<String> previous = t.hget(name, (String) key);
        t.hdel(name, (String) key);
        t.exec();
        return previous.get();
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        // Quick dirty way
//        for(Entry<? extends String, ? extends String> data : m.entrySet()){
//            put(data.getKey(), data.getValue());
//        }
        // better way
        final AbstractTransaction t = jedisPooled.multi();
        for(Entry<? extends String, ? extends String> entry : m.entrySet()){
            t.hset(name, entry.getKey(), entry.getValue());
        }
//            m.entrySet().
//                    forEach( entry ->
//                            t.hset(name, entry.getKey(), entry.getValue())
//                    );
        t.exec();
    }

    @Override
    public void clear() {
        jedisPooled.del(name);
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
        HScanIterator hScanIterator = new HScanIterator(jedisPooled, name);
        while (hScanIterator.hasNext()){
            keyValues.add(hScanIterator.next());
        }
        return keyValues;
    }

}
