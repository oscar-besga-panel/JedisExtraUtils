package org.oba.jedis.extra.utils.utils;

import redis.clients.jedis.JedisPubSub;

import java.util.function.BiConsumer;


/**
 * A simple JedisPubSub implementation
 * It gets a biconsumer that will receive a message every time onMessage is called
 * The bicosumer will accept channel and message (in that order)
 */
public class SimplePubSub extends JedisPubSub {

    private final BiConsumer<String, String> consumer;

    /**
     * Creates a new JedisPubSub with the function that will receive messages
     * The bicosumer will accept channel and message (in that order)
     * @param consumer function
     */
    public SimplePubSub(BiConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    public void onMessage(String channel, String message) {
        consumer.accept(channel, message);
    }

}
