package org.oba.jedis.extra.utils.notificationLock;

import org.oba.jedis.extra.utils.utils.JedisPoolUser;
import org.oba.jedis.extra.utils.utils.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public class NotificationLockFactory implements Named, JedisPoolUser, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationLockFactory.class);

    private static final List<NotificationLock> EMPTY_LOCK_LIST = new ArrayList<>(0);

    private static final String LOCK_NAME_FORMAT = "%s:%s";

    private final String factoryName;
    private final JedisPool jedisPool;
    private final ExecutorService messagesEvenLaucher;
    private final Map<String, List<NotificationLock>> lockMap;
    private final MessageSystem messageSystem;
    private final AtomicBoolean active = new AtomicBoolean(true);

    private long lastTokenCurrentTimeMilis;

    public NotificationLockFactory(String factoryName, JedisPool jedisPool) {
        this(factoryName, jedisPool, true);
    }

    NotificationLockFactory(String factoryName, JedisPool jedisPool, boolean useStreams) {
        this.factoryName = factoryName;
        this.jedisPool = jedisPool;
        this.lockMap = new HashMap<>();
        messagesEvenLaucher = Executors.newSingleThreadExecutor();
        if (useStreams) {
            messageSystem = new StreamMessageSystem(this);
        } else {
            messageSystem = new PubSubMessageSystem(this);
            throw new IllegalStateException("MESSAGES COULD BE LOST");
        }
    }

    public void onMessage(String channel, String message) {
        if (active.get()) {
            LOGGER.debug("onMessage channel {} message {}", channel, message);
            lockMap.getOrDefault(message, EMPTY_LOCK_LIST).
                    forEach(NotificationLock::awake);
        }
    }

    void messageOnUnlock(NotificationLock notificationLock) {
        if (active.get()) {
            LOGGER.debug("messageOnUnlock notificationLock {}", notificationLock);
            messageSystem.sendMessage(notificationLock.getName());
        }
    }

    @Override
    public String getName() {
        return factoryName;
    }

    @Override
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public void close() throws Exception {
        active.set(false);
        messagesEvenLaucher.shutdown();
        messagesEvenLaucher.shutdownNow();
        messageSystem.close();
        lockMap.values().forEach(nll ->
                nll.forEach(NotificationLock::close)
        );
    }

    public NotificationLock createNewLock(String lockName) {
        if (active.get()) {
            LOGGER.debug("createNewLock lockName {}", lockName);
            String currentLockName = String.format(LOCK_NAME_FORMAT, factoryName, lockName);
            String uniqueToken = generateUniqueToken(currentLockName);
            NotificationLock notificationLock = new NotificationLock(this, currentLockName, uniqueToken);
            lockMap.computeIfAbsent(lockName, k -> new ArrayList<>()).add(notificationLock);
            return notificationLock;
        } else {
            LOGGER.debug("createNewLock lockName {} ERROR NOT ACTIVE", lockName);
            throw new IllegalStateException("Can not create lock, factory is closed");
        }
    }

    private synchronized String generateUniqueToken(String name){
        long currentTimeMillis = System.currentTimeMillis();
        while(currentTimeMillis == lastTokenCurrentTimeMilis){
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                //NOOP
            }
            currentTimeMillis = System.currentTimeMillis();
        }
        lastTokenCurrentTimeMilis = currentTimeMillis;
        return name + "_" + System.currentTimeMillis() + "_" + ThreadLocalRandom.current().nextInt(1_000_000);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NotificationLockFactory that = (NotificationLockFactory) o;
        return Objects.equals(factoryName, that.factoryName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(factoryName);
    }

    public static interface MessageSystem extends JedisPoolUser, AutoCloseable {

        void onMessage(String message);

        void sendMessage(String message);
    }

}
