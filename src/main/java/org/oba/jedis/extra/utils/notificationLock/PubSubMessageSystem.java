package org.oba.jedis.extra.utils.notificationLock;

import org.oba.jedis.extra.utils.utils.SimplePubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * DO NOT USE
 * It can lose messages between reads
 * (as streams won't because they use the ID to have the last one from read operation)
 */
public class PubSubMessageSystem implements NotificationLockFactory.MessageSystem {

    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubMessageSystem.class);

    private final NotificationLockFactory factory;

    private final Thread messagesThread;
    private final AtomicBoolean active = new AtomicBoolean(true);
    private final SimplePubSub pubSub;

    PubSubMessageSystem(NotificationLockFactory factory) {
        this.factory = factory;
        this.pubSub = new SimplePubSub(this::receiveMessage);
        this.messagesThread = new Thread(this::listenMessages);
        this.messagesThread.setDaemon(true);
        this.messagesThread.setName("PubSubMessageSystem_MessagesThread_" + factory.getName());
        //this.messagesThread.start();
        throw new IllegalStateException("MESSAGES COULD BE LOST");
    }

    void listenMessages(){
        withJedisPoolDo(jedis -> {
            LOGGER.debug("subscribe on");
            jedis.subscribe(pubSub, factory.getName());
            LOGGER.debug("subscribe off");
        });
    }

    void receiveMessage(String channel, String message) {
        LOGGER.debug("receiveMessage channel {} message {}", channel, message);
        if (factory.getName().equals(channel) && active.get()) {
            onMessage(message);
        }
    }

    @Override
    public void onMessage(String message) {
        LOGGER.debug("onMessage message {}", message);
        factory.onMessage(factory.getName(), message);
    }

    @Override
    public void sendMessage(String message) {
        LOGGER.debug("sendMessage channel {} message {}", factory.getName(), message);
        if (active.get()) {
            withJedisPoolDo(jedis -> jedis.publish(factory.getName(), message));
        }
    }

    @Override
    public void close() throws Exception {
        LOGGER.debug("close");
        active.set(false);
        pubSub.unsubscribe();
    }

    @Override
    public JedisPool getJedisPool() {
        return factory.getJedisPool();
    }

}