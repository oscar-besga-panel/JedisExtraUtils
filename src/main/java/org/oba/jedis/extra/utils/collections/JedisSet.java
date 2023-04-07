package org.oba.jedis.extra.utils.collections;

import org.oba.jedis.extra.utils.iterators.SScanIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class JedisSet implements Set<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisSet.class);


    private final JedisPool jedisPool;
    private final String name;

    public JedisSet(JedisPool jedisPool, String name){
        this.jedisPool = jedisPool;
        this.name = name;
    }

    /**
     * Name of the redis set
     * @return redis name
     */
    public String getName() {
        return name;
    }


    /**
     * If set exist in Redis namespace
     * @return true if there is a reference in redis namespace, false otherwise
     */
    public boolean exists() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(name);
        }
    }

    /**
     * Checks if set exist in Redis namespace
     * @throws IllegalStateException if there is not a reference in redis namespace
     */
    public void checkExists() {
        if (!exists()) {
            throw new IllegalStateException("Current set  " + name + " not found in redis server");
        }
    }

    /**
     * Returns a set in java memory with the data of the set on redis
     * It copies the redis data in java process
     * (current implementation is an Hashset, this may change)
     * @return map of data
     */
    public Set<String> asSet(){
        return new HashSet<>(doSscan());
    }

    @Override
    public int size() {
        try (Jedis jedis = jedisPool.getResource()) {
            long value = jedis.scard(name);
            return Long.valueOf(value).intValue();
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.sismember(name, (String) o);
        }
    }

    @Override
    public Iterator<String> iterator() {
        return new SScanIterator(jedisPool, name);
    }

    @Override
    public Object[] toArray() {
        return doSscan().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return doSscan().toArray(a);
    }

    @Override
    public boolean add(String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            Long result = jedis.sadd(name, value);
            return result != 0L;
        }
    }

    @Override
    public boolean remove(Object o) {
        try (Jedis jedis = jedisPool.getResource()) {
            Long result = jedis.srem(name, (String) o);
            return result != 0L;
        }
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        try (Jedis jedis = jedisPool.getResource()) {
            boolean result = true;
            Iterator<?> it = c.iterator();
            while (result && it.hasNext()){
                result = jedis.sismember(name, (String) it.next());
            }
            return result;
        }
    }

    @Override
    public boolean addAll(Collection<? extends String> values) {
        if (values == null) {
            throw new IllegalArgumentException("values is null");
        } else if (values.isEmpty()) {
            return false;
        } else {
            String[] arrayValues = values.toArray(new String[0]);
            try (Jedis jedis = jedisPool.getResource()) {
                Long result = jedis.sadd(name, arrayValues);
                return result != 0L;
            }
        }
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> retained = new HashSet<>(doSscan());
            boolean result = retained.retainAll(c);
            if (result) {
                Transaction t = jedis.multi();
                t.del(name);
                retained.forEach( s -> t.sadd(name, s));
                t.exec();
            }
            return result;
        }
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        try (Jedis jedis = jedisPool.getResource()) {
            String[] a = c.toArray(new String[0]);
            Long result = jedis.srem(name, a);
            return result != 0L;
        }
    }

    @Override
    public void clear() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(name);
        }
    }

    private Set<String> doSscan() {
        Set<String> values = new HashSet<>();
        SScanIterator sScanIterator = new SScanIterator(jedisPool, name);
        while(sScanIterator.hasNext()){
            values.add(sScanIterator.next());
        }
        return values;
    }

}