package org.obapanel.jedis.collections;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class JedisSet implements Set<String> {

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
     * Returns a set in java memory with the data of the set on redis
     * It copies the redis data in java process
     * (current implementation is an Hashset, this may change)
     * @return map of data
     */
    //TODO test
    public Set<String> asSet(){
        Set<String> data = new HashSet<>();
        data.addAll(doSscan());
        return data;
    }

    @Override
    public int size() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.scard(name).intValue();
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
        return null;
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
        return false;
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
        String[] arrayValues = values.toArray(new String[0]);
        try (Jedis jedis = jedisPool.getResource()) {
            Long result = jedis.sadd(name, arrayValues);
            return result != 0L;
        }
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(name);
        }
    }

    private Set<String> doSscan() {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> keyValues = new HashSet<>();
            ScanParams scanParams = new ScanParams(); // Scan on two-by-two responses
            String cursor = ScanParams.SCAN_POINTER_START;
            do {
                ScanResult<String> partialResult =  jedis.sscan(name, cursor, scanParams);
                cursor = partialResult.getCursor();
                keyValues.addAll(partialResult.getResult());
            }  while(!cursor.equals(ScanParams.SCAN_POINTER_START));
            return keyValues;
        }
    }

    private class JedisSetIterator implements Iterator<String> {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public String next() {
            return null;
        }

        @Override
        public void remove() {

        }
    }
}
