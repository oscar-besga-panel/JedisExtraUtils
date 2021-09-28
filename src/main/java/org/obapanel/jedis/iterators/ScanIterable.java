package org.obapanel.jedis.iterators;

import redis.clients.jedis.JedisPool;

import java.util.Spliterator;
import java.util.function.Consumer;

public class ScanIterable implements Iterable<String> {

    private final JedisPool jedisPool;
    private final String pattern;
    private final int resultsPerScan;

    public ScanIterable(JedisPool jedisPool){
        this(jedisPool,"", 1);
    }

    public ScanIterable(JedisPool jedisPool, String pattern){
        this(jedisPool, pattern, 1);
    }

    public ScanIterable(JedisPool jedisPool, String pattern, int resultsPerScan){
        this.jedisPool = jedisPool;
        this.pattern = pattern;
        this.resultsPerScan = resultsPerScan;
    }


    @Override
    public ScanIterator iterator() {
        return new ScanIterator(jedisPool, pattern, resultsPerScan);
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
