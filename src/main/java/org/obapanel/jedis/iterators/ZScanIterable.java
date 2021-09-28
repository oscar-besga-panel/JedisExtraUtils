package org.obapanel.jedis.iterators;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Tuple;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

public class ZScanIterable implements Iterable<Tuple> {


    private final JedisPool jedisPool;
    private final String name;
    private final String pattern;
    private final int resultsPerScan;

    public ZScanIterable(JedisPool jedisPool, String name){
        this(jedisPool, name, "", 1);
    }

    public ZScanIterable(JedisPool jedisPool, String name, String pattern){
        this(jedisPool, name, pattern, 1);
    }

    public ZScanIterable(JedisPool jedisPool, String name, String pattern, int resultsPerScan){
        this.jedisPool = jedisPool;
        this.name = name;
        this.pattern = pattern;
        this.resultsPerScan = resultsPerScan;
    }


    @Override
    public Iterator<Tuple> iterator() {
        return new ZScanIterator(jedisPool, name, pattern, resultsPerScan);
    }

    @Override
    public void forEach(Consumer<? super Tuple> action) {
        Iterable.super.forEach(action);
    }

    @Override
    public Spliterator<Tuple> spliterator() {
        return Iterable.super.spliterator();
    }
}
