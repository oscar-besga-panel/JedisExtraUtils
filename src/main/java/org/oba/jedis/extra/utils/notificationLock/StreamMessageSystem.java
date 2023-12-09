package org.oba.jedis.extra.utils.notificationLock;

import org.oba.jedis.extra.utils.utils.JedisPoolUser;
import org.oba.jedis.extra.utils.utils.NamedMessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class StreamMessageSystem implements JedisPoolUser, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamMessageSystem.class);

    private static final AtomicLong num = new AtomicLong();

    private static final int XREADPARAMS_BLOCK = 5000;
    private static final int XREADPARAMS_COUNT = 1;
    private static final XReadParams XREADPARAMS = new XReadParams().
            block(XREADPARAMS_BLOCK).count(XREADPARAMS_COUNT);

    private static final StreamEntryID MAX_RANGE = null;
    public static final String MESSAGE = "message";
    public static final String TS = "ts";
    public static final String RND = "rnd";



    private final NamedMessageListener messageListener;
    private final JedisPool jedisPool;

    private final Thread messagesThread;
    private final AtomicBoolean active = new AtomicBoolean(true);
    private Map<String,String> lastMessageSent;
    private StreamEntryID lastStreamEntryIDSent;
    private StreamEntryID lastStreamEntryIDRange;

    protected StreamMessageSystem(NamedMessageListener messageListener, JedisPool jedisPool) {
        this.messageListener = messageListener;
        this.jedisPool = jedisPool;
        this.messagesThread = new Thread(this::listenMessages);
        this.messagesThread.setDaemon(true);
        this.messagesThread.setName("StreamMessageSystem_MessagesThread_" + messageListener + "_" + num.incrementAndGet());
        this.messagesThread.start();
    }

    public void listenMessages() {
        try {
            listenMessagesWithXREAD();
        } catch (Exception e) {
            LOGGER.error("Error in messagesThread", e);
        }
    }

    public void listenMessagesWithXREAD() {
        withJedisPoolDo(jedis -> {
            while (active.get()){
                Map<String, StreamEntryID> streamData;
                if (lastStreamEntryIDRange == null) {
                    streamData = Map.of(messageListener.getName(), StreamEntryID.LAST_ENTRY);
                } else {
                    streamData = Map.of(messageListener.getName(), nextStreamEntryID(lastStreamEntryIDRange));
                }
                LOGGER.debug("listenMessages begin {} with streamdata {}", messageListener.getName(), streamData);
                List<Map.Entry<String, List<StreamEntry>>> streamedEntries = jedis.xread(XREADPARAMS, streamData);
                if (streamedEntries != null && !streamedEntries.isEmpty()) {
                    streamedEntries.stream().
                            filter( streamedEntry -> streamedEntry.getKey().equals(messageListener.getName())).
                            map(Map.Entry::getValue).
                            forEach(this::processEntries);
                }
                LOGGER.debug("listenMessages end {} with streamdata {} as result {}", messageListener.getName(), streamData, streamedEntries);
            }
        });
    }

    /**
     * Not used because polling
     */
    public void listenMessagesWithXRANGE() {
        withJedisPoolDo(jedis -> {
            while(active.get()) {
                StreamEntryID currentRange = nextStreamEntryID(lastStreamEntryIDRange);
                LOGGER.debug("listenMessages begin {} with currentRange {}", messageListener.getName(), currentRange);
                List<StreamEntry> entries = jedis.xrange(messageListener.getName(), currentRange, MAX_RANGE);
                processEntries(entries);
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    LOGGER.error("Interruted but not relauched", e);
                }
                LOGGER.debug("listenMessages end   {}", messageListener.getName());
            }
        });
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
            sentMessage = lastMessageSent.get(MESSAGE).equals(entry.getFields().get(MESSAGE)) &&
                    lastMessageSent.get(TS).equals(entry.getFields().get(TS)) &&
                    lastMessageSent.get(RND).equals(entry.getFields().get(RND)) &&
                    lastStreamEntryIDSent.equals(entry.getID());
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
            Map<String, String> data = Map.of(MESSAGE, message,
                    TS, Long.toString(System.currentTimeMillis()),
                    RND, Integer.toString(ThreadLocalRandom.current().nextInt(1_000_000)));
            lastMessageSent = data;
            lastStreamEntryIDSent = jedis.xadd(messageListener.getName(), addParams, data);
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
