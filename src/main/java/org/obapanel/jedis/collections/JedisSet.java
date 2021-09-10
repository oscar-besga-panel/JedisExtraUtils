package org.obapanel.jedis.collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
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
        return new JedisSetIterator();
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

    private static final ScanParams SCANPARAMS_ONE_COUNT = new ScanParams().count(1);


    private class JedisSetIterator implements Iterator<String> {

        private final Queue<String> nextValues = new LinkedList<>();
        private ScanResult<String> currentResult;
        private String next;
        private final Set<String> alreadyRecoveredData = new HashSet<>();

        @Override
        public boolean hasNext() {
            if (nextValues.isEmpty()) {
                nextValues.addAll(moreValues());
            }
            return !nextValues.isEmpty();
        }

        List<String> moreValues() {
            String currentCursor;
            if (currentResult == null) {
                currentCursor = ScanParams.SCAN_POINTER_START;
            } else {
                currentCursor = currentResult.getCursor();
            }
            LOGGER.debug("Petition with currentCursor " + currentCursor);
            try (Jedis jedis = jedisPool.getResource()) {
                currentResult = jedis.sscan(name, currentCursor, SCANPARAMS_ONE_COUNT);
            }
            LOGGER.debug("Recovered data list is {}  with cursor {} ", currentResult.getResult(), currentResult.getCursor());
            return filterResultToAvoidDuplicated(currentResult.getResult());
        }

        List<String> filterResultToAvoidDuplicated(List<String> currentData) {
            List<String> filteredData = new ArrayList<>();
            for(String s: currentData) {
                if (s != null && !s.isEmpty() &&
                        alreadyRecoveredData.add(s)) {
                    filteredData.add(s);
                }
            }
            return filteredData;
        }

        @Override
        public String next() {
            next =  nextValues.poll();
            LOGGER.debug("Data next {} ", next);
            return next;
        }

        public void remove() {
            if (next != null) {
                try (Jedis jedis = jedisPool.getResource()) {
                    jedis.srem(name, next);
                }
            } else {
                throw new IllegalStateException("Next not called or other error");
            }
        }
    }

}