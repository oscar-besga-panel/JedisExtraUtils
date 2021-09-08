package org.obapanel.jedis.collections;

import redis.clients.jedis.JedisPool;

import java.util.Collection;
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


    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @Override
    public Iterator<String> iterator() {
        return null;
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return null;
    }

    @Override
    public boolean add(String s) {
        return false;
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends String> c) {
        return false;
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
