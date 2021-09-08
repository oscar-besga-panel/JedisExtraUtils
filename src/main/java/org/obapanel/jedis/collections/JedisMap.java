package org.obapanel.jedis.collections;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class JedisMap implements Map<String, String> {


    private final JedisPool jedisPool;
    private final String name;

    public JedisMap(JedisPool jedisPool, String name){
        this.jedisPool = jedisPool;
        this.name = name;
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

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
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
        return null;
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {

    }

    @Override
    public void clear() {

    }

    @Override
    public Set<String> keySet() {
        return null;
    }

    @Override
    public Collection<String> values() {
        return null;
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return null;
    }
}
