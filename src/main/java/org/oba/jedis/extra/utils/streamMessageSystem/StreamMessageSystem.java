package org.oba.jedis.extra.utils.streamMessageSystem;

import org.oba.jedis.extra.utils.utils.JedisPoolUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class StreamMessageSystem implements JedisPoolUser, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamMessageSystem.class);

    private static final AtomicLong num = new AtomicLong();

    private static final int XREADPARAMS_BLOCK = 5000;
    private static final int XREADPARAMS_COUNT = 1;
    private static final XReadParams XREADPARAMS = new XReadParams().
            block(XREADPARAMS_BLOCK).count(XREADPARAMS_COUNT);
    public static final String MESSAGE = "message";


    private final String name;
    private final MessageListener messageListener;
    private final JedisPool jedisPool;

    private final Thread messagesThread;
    private final AtomicBoolean active = new AtomicBoolean(true);
    private Map<String,String> lastMessageSent;
    private StreamEntryID lastStreamEntryIDSent;
    private StreamEntryID lastStreamEntryIDRange;

    public StreamMessageSystem(String name, JedisPool jedisPool, MessageListener messageListener) {
        this.name = name;
        this.messageListener = messageListener;
        this.jedisPool = jedisPool;
        this.messagesThread = new Thread(this::listenMessages);
        this.messagesThread.setDaemon(true);
        this.messagesThread.setName("StreamMessageSystem_MessagesThread_" + messageListener + "_" + num.incrementAndGet());
        this.messagesThread.start();
    }

    public void listenMessages() {
        try {
            withJedisPoolDo(this::listenMessagesWithXREAD);
        } catch (Exception e) {
            LOGGER.error("Error in messagesThread", e);
        }
    }

    void listenMessagesWithXREAD(Jedis jedis) {
        while (active.get()){
            Map<String, StreamEntryID> streamData;
            if (lastStreamEntryIDRange == null) {
                streamData = Map.of(name, StreamEntryID.LAST_ENTRY);
            } else {
                streamData = Map.of(name, nextStreamEntryID(lastStreamEntryIDRange));
            }
            // LOGGER.debug("listenMessages begin {} with streamdata {}", name, streamData);
            List<Map.Entry<String, List<StreamEntry>>> streamedEntries = jedis.xread(XREADPARAMS, streamData);
            if (streamedEntries != null && !streamedEntries.isEmpty()) {
                streamedEntries.stream().
                        filter( streamedEntry -> streamedEntry.getKey().equals(name)).
                        map(Map.Entry::getValue).
                        forEach(this::processEntries);
            }
            // LOGGER.debug("listenMessages end {} with streamdata {} as result {}", name, streamData, streamedEntries);
        }
    }

    private void processEntries(List<StreamEntry> entries) {
        entries.forEach(this::processEntry);
    }

    private void processEntry(StreamEntry entry) {
        LOGGER.debug("listenMessages entry {}", entry);
        lastStreamEntryIDRange = entry.getID();
        if (checkIfNotSent(entry)) {
            String message = entry.getFields().get(MESSAGE);
            LOGGER.debug("onMessage message {}", message);
            messageListener.onMessage(message);
        }
    }

    synchronized boolean checkIfNotSent(StreamEntry entry) {
        boolean sentMessage = false;
        if (lastMessageSent != null && lastStreamEntryIDSent != null){
            sentMessage = lastStreamEntryIDSent.equals(entry.getID()) &&
                    lastMessageSent.get(MESSAGE).equals(entry.getFields().get(MESSAGE));
            if (sentMessage) {
                lastMessageSent = null;
                lastStreamEntryIDSent = null;
            }
        }
        LOGGER.debug("checkMessage equalToLast");
        return !sentMessage;
    }

    public synchronized void sendMessage(String message) {
        withJedisPoolDo(jedis -> {
            XAddParams addParams = new XAddParams();
            Map<String, String> data = Map.of(MESSAGE, message);
            lastMessageSent = data;
            lastStreamEntryIDSent = jedis.xadd(name, addParams, data);
            LOGGER.debug("sendMessage message {} with lastStreamEntryIDSent {}", message, lastStreamEntryIDSent);
        });
    }

    @Override
    public void close() throws Exception {
        active.set(false);
        messagesThread.interrupt();
    }

    @Override
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public static StreamEntryID nextStreamEntryID(StreamEntryID current) {
        if (current == null) {
            return null;
        } else {
            return new StreamEntryID(current.getTime(), current.getSequence() + 1);
        }
    }

}
