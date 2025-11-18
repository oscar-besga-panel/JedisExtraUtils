package org.oba.jedis.extra.utils.streamMessageSystem;

import org.oba.jedis.extra.utils.utils.JedisPoolUser;
import org.oba.jedis.extra.utils.utils.Named;
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

/**
 * A stream message system that reads messages from a stream and can send them
 * When a message is read, the listener is invoked by the onMessage method
 * Messages sent don't trigger listener
 * Listener is invoked by a background thread, and the connection waits for a new message to arrive
 * No messages are lost in the stream, all are retrieved and passed to the listener
 */
public final class StreamMessageSystem implements JedisPoolUser, Named, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamMessageSystem.class);

    private static final AtomicLong num = new AtomicLong();

    private static final int XREADPARAMS_BLOCK = 60_000; // 1 minute is a looong time in redis
    private static final int XREADPARAMS_COUNT = 1;
    public static final String MESSAGE = "message";


    private final String name;
    private final MessageListener messageListener;
    private final JedisPool jedisPool;
    private final int blockMillis;

    private final Thread messagesThread;
    private final AtomicBoolean active = new AtomicBoolean(true);
    private Map<String,String> lastMessageSent;
    private StreamEntryID lastStreamEntryIDSent;
    private StreamEntryID lastStreamEntryIDRange;

    /**
     * Creates new
     * @param name Name of the stream
     * @param jedisPool Pool of connections
     * @param messageListener Listener to messages
     */
    public StreamMessageSystem(String name, JedisPool jedisPool, MessageListener messageListener) {
        this(name, jedisPool, XREADPARAMS_BLOCK, messageListener);
    }

    /**
     * Creates new
     * @param name Name of the stream
     * @param jedisPool Pool of connections
     * @param blockMillis Time to block connection
     * @param messageListener Listener to messages
     */
    public StreamMessageSystem(String name, JedisPool jedisPool, int blockMillis, MessageListener messageListener) {
        this.name = name;
        this.messageListener = messageListener;
        this.jedisPool = jedisPool;
        this.blockMillis = blockMillis;
        this.messagesThread = new Thread(this::listenMessages);
        this.messagesThread.setDaemon(true);
        this.messagesThread.setName("StreamMessageSystem_MessagesThread_" + messageListener + "_" + num.incrementAndGet());
        this.messagesThread.start();
    }

    /**
     * Internal method of thread
     */
    void listenMessages() {
        try {
            withJedisPoolDo(this::listenMessagesWithXREAD);
        } catch (Exception e) {
            LOGGER.error("Error in messagesThread", e);
        }
    }

    /**
     * Creates new params for the reading
     * @return params for reading
     */
    XReadParams newXReadParams() {
        return new XReadParams().
                block(blockMillis).count(XREADPARAMS_COUNT);
    }

    /**
     * Internal method of thread
     */
    void listenMessagesWithXREAD(Jedis jedis) {
        while (active.get()){
            Map<String, StreamEntryID> streamData;
            if (lastStreamEntryIDRange == null) {
                streamData = Map.of(name, StreamEntryID.LAST_ENTRY);
            } else {
                streamData = Map.of(name, nextStreamEntryID(lastStreamEntryIDRange));
            }
            // LOGGER.debug("listenMessages begin {} with streamdata {}", name, streamData);
            List<Map.Entry<String, List<StreamEntry>>> streamedEntries = null;
            if (active.get()) {
                streamedEntries = jedis.xread(newXReadParams(), streamData);
            }
            if (active.get() && streamedEntries != null && !streamedEntries.isEmpty()) {
                streamedEntries.stream().
                        filter( streamedEntry -> streamedEntry.getKey().equals(name)).
                        map(Map.Entry::getValue).
                        forEach(this::processEntries);
            }
            // LOGGER.debug("listenMessages end {} with streamdata {} as result {}", name, streamData, streamedEntries);
        }
    }

    /**
     * Interal method to process the list of arrived messages
     * @param entries to process
     */
    void processEntries(List<StreamEntry> entries) {
        entries.forEach(this::processEntry);
    }

    /**
     * Interal method to process an entry of a messages
     * If messge is not sent, it is delivered to listeners
     * @param entry to process
     */
    void processEntry(StreamEntry entry) {
        LOGGER.debug("listenMessages entry {}", entry);
        lastStreamEntryIDRange = entry.getID();
        if (active.get() && checkIfNotSent(entry)) {
            String message = entry.getFields().get(MESSAGE);
            LOGGER.debug("onMessage message {}", message);
            messageListener.onMessage(message);
        }
    }

    /**
     * Checks if a message was sent by this messagesystem
     * @param entry entry to checkk
     * @return true if NOT sent from here
     */
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

    /**
     * Send a message to this stream.
     * Stores it's id and text to check if sent from here
     * @param message message to sent
     */
    public void sendMessage(String message) {
        if (!active.get()) {
            throw new IllegalStateException("StreamMessageSystem not active, cannot send!");
        }
        withJedisPoolDo(jedis -> {
            XAddParams addParams = new XAddParams();
            Map<String, String> data = Map.of(MESSAGE, message);
            lastMessageSent = data;
            lastStreamEntryIDSent = jedis.xadd(name, addParams, data);
            LOGGER.debug("sendMessage message {} with lastStreamEntryIDSent {}", message, lastStreamEntryIDSent);
        });
    }

    @Override
    public void close() {
        active.set(false);
        messagesThread.interrupt();
    }

    @Override
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Calculates the next stream id to the current one
     * Sequence plus one
     * @param current current stream id
     * @return next stream id
     */
    public static StreamEntryID nextStreamEntryID(StreamEntryID current) {
        if (current == null) {
            return null;
        } else {
            return new StreamEntryID(current.getTime(), current.getSequence() + 1);
        }
    }

}
