package org.obapanel.jedis.iterators;

import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;

public class HScanIterable implements Iterable<Map.Entry<String,String>> {

    private final JedisPool jedisPool;
    private final String name;
    private final String pattern;
    private final int resultsPerScan;

    public HScanIterable(JedisPool jedisPool, String name){
        this(jedisPool, name, "", 1);
    }

    public HScanIterable(JedisPool jedisPool, String name, String pattern){
        this(jedisPool, name, pattern, 1);
    }

    public HScanIterable(JedisPool jedisPool, String name, String pattern, int resultsPerScan){
        this.jedisPool = jedisPool;
        this.name = name;
        this.pattern = pattern;
        this.resultsPerScan = resultsPerScan;
    }


    @Override
    public HScanIterator iterator() {
        return new HScanIterator(jedisPool, name, pattern, resultsPerScan);
    }

    @Override
    public void forEach(Consumer<? super Map.Entry<String,String>> action) {
        Iterable.super.forEach(action);
    }

    @Override
    public Spliterator<Map.Entry<String,String>> spliterator() {
        return Iterable.super.spliterator();
    }

}
