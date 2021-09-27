package org.obapanel.jedis.iterators;

import redis.clients.jedis.JedisPool;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

public class SScanIterable implements Iterable<String> {


    private final JedisPool jedisPool;
    private final String name;
    private final String pattern;
    private final int resultsPerScan;

    public SScanIterable(JedisPool jedisPool, String name){
        this(jedisPool, name, "", 1);
    }

    public SScanIterable(JedisPool jedisPool, String name, String pattern){
        this(jedisPool, name, pattern, 1);
    }

    public SScanIterable(JedisPool jedisPool, String name, String pattern, int resultsPerScan){
        this.jedisPool = jedisPool;
        this.name = name;
        this.pattern = pattern;
        this.resultsPerScan = resultsPerScan;
    }


    @Override
    public Iterator<String> iterator() {
        return new SScanIterator(jedisPool, name, pattern, resultsPerScan);
    }

    @Override
    public void forEach(Consumer<? super String> action) {
        Iterable.super.forEach(action);
    }

    @Override
    public Spliterator<String> spliterator() {
        return Iterable.super.spliterator();
    }
}
