package org.oba.jedis.extra.utils.collections;

import org.oba.jedis.extra.utils.iterators.SScanIterator;
import org.oba.jedis.extra.utils.utils.Named;
import redis.clients.jedis.AbstractTransaction;
import redis.clients.jedis.UnifiedJedis;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public final class JedisSet implements Set<String>, Named {

    private final UnifiedJedis redisClient;
    private final String name;

    public JedisSet(UnifiedJedis redisClient, String name){
        this.redisClient = redisClient;
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
        return redisClient.exists(name);
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
        long value = redisClient.scard(name);
        return Long.valueOf(value).intValue();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        return redisClient.sismember(name, (String) o);
    }

    @Override
    public Iterator<String> iterator() {
        return new SScanIterator(redisClient, name);
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
        long result = redisClient.sadd(name, value);
        return result != 0L;
    }

    @Override
    public boolean remove(Object o) {
        long result = redisClient.srem(name, (String) o);
        return result != 0L;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        boolean result = true;
        Iterator<?> it = c.iterator();
        while (result && it.hasNext()){
            result = redisClient.sismember(name, (String) it.next());
        }
        return result;
    }

    @Override
    public boolean addAll(Collection<? extends String> values) {
        if (values == null) {
            throw new IllegalArgumentException("values is null");
        } else if (values.isEmpty()) {
            return false;
        } else {
            String[] arrayValues = values.toArray(new String[0]);
            long result = redisClient.sadd(name, arrayValues);
            return result != 0L;
        }
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        Set<String> retained = new HashSet<>(doSscan());
        boolean result = retained.retainAll(c);
        if (result) {
            try (AbstractTransaction t = redisClient.multi()) {
                t.del(name);
                retained.forEach( s -> t.sadd(name, s));
                t.exec();
            }
        }
        return result;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        String[] a = c.toArray(new String[0]);
        long result = redisClient.srem(name, a);
        return result != 0L;
    }

    @Override
    public void clear() {
        redisClient.del(name);
    }

    private Set<String> doSscan() {
        Set<String> values = new HashSet<>();
        SScanIterator sScanIterator = new SScanIterator(redisClient, name);
        while(sScanIterator.hasNext()){
            values.add(sScanIterator.next());
        }
        return values;
    }

}