package org.obapanel.jedis.utils;


import redis.clients.jedis.JedisPubSub;

import java.util.function.BiConsumer;

public class OnMessagePubSub extends JedisPubSub {

    private final BiConsumer<String, String> onMessageDo;

    public OnMessagePubSub(BiConsumer<String, String> onMessageDo) {
        this.onMessageDo = onMessageDo;
    }

    public void onMessage(String channel, String message) {
        onMessageDo.accept(channel, message);
    }

}