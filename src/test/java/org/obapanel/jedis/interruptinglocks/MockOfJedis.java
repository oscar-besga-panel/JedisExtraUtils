package org.obapanel.jedis.interruptinglocks;

import org.mockito.Mockito;
import org.obapanel.jedis.common.test.TransactionOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.util.SafeEncoder;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.obapanel.jedis.common.test.TTL.wrapTTL;
import static redis.clients.jedis.Protocol.Keyword.MESSAGE;
import static redis.clients.jedis.Protocol.Keyword.SUBSCRIBE;
import static redis.clients.jedis.Protocol.Keyword.UNSUBSCRIBE;

/**
 * Mock of jedis methods used by the lock
 *
 * To allow or disallow unit tests
 * change the UNIT_TEST_CYCLES variable to 1 or more (allow) or zero (disallow)
 */
public class MockOfJedis {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockOfJedis.class);

    public static final String CLIENT_RESPONSE_OK = "OK";
    public static final String CLIENT_RESPONSE_KO = "KO";


    // Zero to prevent some unit test
    // One to one pass
    // More to more passes
    public static final int UNIT_TEST_CYCLES = 1;

    static boolean unitTestEnabled(){
        return UNIT_TEST_CYCLES > 0;
    }


    static boolean checkLock(IJedisLock jedisLock){
        LOGGER.info("interruptingLock.isLocked() " + jedisLock.isLocked() + " for thread " + Thread.currentThread().getName());
        if (jedisLock.isLocked()) {
            LOGGER.debug("LOCKED");
            return true;
        } else {
            IllegalStateException ise =  new IllegalStateException("LOCK NOT ADQUIRED isLocked " + jedisLock.isLocked());
            LOGGER.error("ERROR LOCK NOT ADQUIRED e {} ", ise.getMessage(), ise);
            throw ise;
        }
    }

    static boolean checkLock(java.util.concurrent.locks.Lock lock){
        if (lock instanceof  org.obapanel.jedis.interruptinglocks.Lock) {
            org.obapanel.jedis.interruptinglocks.Lock jedisLock = (org.obapanel.jedis.interruptinglocks.Lock) lock;
            LOGGER.info("interruptingLock.isLocked() " + jedisLock.isLocked() + " for thread " + Thread.currentThread().getName());
            if (jedisLock.isLocked()) {
                LOGGER.debug("LOCKED");
                return true;
            } else {
                IllegalStateException ise =  new IllegalStateException("LOCK NOT ADQUIRED isLocked " + jedisLock.isLocked());
                LOGGER.error("ERROR LOCK NOT ADQUIRED e {} ", ise.getMessage(), ise);
                throw ise;
            }
        } else {
            return true;
        }
    }

    private final Jedis jedis;
    private final JedisPool jedisPool;
    private final Transaction transaction;
    private final List<TransactionOrder<String>> transactionActions = new ArrayList<>();
    private final Map<String, String> data = Collections.synchronizedMap(new HashMap<>());
    private final Map<JedisPubSub, List<String>> subscriptionChannelMap = new HashMap<>();
    private final Map<Client, JedisPubSub> subscriptionClientMap = new HashMap<>();
    private final Map<Client, BlockingQueue<List<Object>>> subscriptionClientQueueMap = new HashMap<>();
    private final Queue<Runnable> doAfterUnflushedObjectMultiBulkReply = new LinkedList<>();
    private final Executor publishExecutor = Executors.newFixedThreadPool(5);
    private final Timer timer = new Timer();

    public MockOfJedis() {
        jedis = Mockito.mock(Jedis.class);
        jedisPool = Mockito.mock(JedisPool.class);
        transaction = Mockito.mock(Transaction.class);
        Mockito.when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            SetParams setParams = ioc.getArgument(2);
            return mockSet(key, value, setParams);

        });
        Mockito.when(jedisPool.getResource()).thenReturn(jedis);
        Mockito.when(jedis.get(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockGet(key);
        });
        Mockito.when(jedis.eval(anyString(),any(List.class), any(List.class))).thenAnswer(ioc -> {
            String script = ioc.getArgument(0);
            List<String> keys = ioc.getArgument(1);
            List<String> values = ioc.getArgument(2);
            return mockEval(script, keys, values);
        });
        Mockito.when(jedis.multi()).thenReturn(transaction);
        Mockito.when(transaction.get(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockTransactionGet(key);
        });
        Mockito.when(transaction.set(anyString(), anyString(), any(SetParams.class))).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            SetParams setParams = ioc.getArgument(2);
            return mockTransactionSet(key, value, setParams);
        });
        Mockito.when(transaction.exec()).thenAnswer(ioc -> mockTransactionExec());
        Mockito.when(jedis.getClient()).thenAnswer(ioc -> newMockClient());
        Mockito.doAnswer(ioc -> {
            JedisPubSub jedisPubSub = ioc.getArgument(0, JedisPubSub.class);
            Object data = ioc.getArgument(1);
            List<String> channels = new ArrayList<>();
            if (data instanceof String) {
                channels.add((String) data);
            } else if (data instanceof String[]) {
                for(String s: (String[]) data){
                    channels.add(s);
                }
            }
            mockSubscribe(jedisPubSub, channels);
            return null;
        }).when(jedis).subscribe(any(JedisPubSub.class), any());
        Mockito.when(jedis.publish(anyString(), anyString())).thenAnswer( ioc -> {
            String channel = ioc.getArgument(0);
            String message = ioc.getArgument(1);
            return mockPublish(channel, message);
        });

    }

    public Client newMockClient() {
        final Client client = Mockito.mock(Client.class);
        subscriptionClientQueueMap.put(client, new LinkedBlockingQueue<>());
        Mockito.doAnswer(ioc -> {
            Object x = ioc.getMock();
            String[] channels = ioc.getArgument(0);
            mockClientSubscribe(client, channels);
            return null;
        }).when(client).subscribe((String[])any());
        Mockito.doAnswer(ioc -> {
            Object x = ioc.getMock();
            String channel = ioc.getArgument(0);
            String[] channels = new String[]{channel};
            mockClientSubscribe(client, channels);
            return null;
        }).when(client).subscribe(anyString());
        Mockito.doAnswer(ioc -> {
            Object x = ioc.getMock();
            String[] channels = ioc.getArgument(0);
            mockClientUnsubscribe(client, channels);
            return null;
        }).when(client).unsubscribe((String[])any());
        Mockito.doAnswer(ioc -> {
            Object x = ioc.getMock();
            String channel = ioc.getArgument(0);
            String[] channels = new String[]{channel};
            mockClientUnsubscribe(client, channels);
            return null;
        }).when(client).unsubscribe(anyString());
        Mockito.doAnswer(ioc -> {
            Object x = ioc.getMock();
            mockClientUnsubscribe(client);
            return null;
        }).when(client).unsubscribe();

        Mockito.when(client.getUnflushedObjectMultiBulkReply()).
                thenAnswer( ioc -> {
                    Object x = ioc.getMock();
                    return mockClientGetUnflushedObjectMultiBulkReply(client);
                });
        return client;
    }

    private List<Object> mockClientGetUnflushedObjectMultiBulkReply(Client client) {
        try {
            BlockingQueue<List<Object>> subscriptionClientQueue = subscriptionClientQueueMap.get(client);
            List<Object> data = subscriptionClientQueue.take();
            Runnable action = doAfterUnflushedObjectMultiBulkReply.poll();
            if (action != null) {
                publishExecutor.execute(action);
            }
            return data;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void mockClientSubscribe(Client client, String[] channels){
        try {
            JedisPubSub jedisPubSub = subscriptionClientMap.get(client);
            List<String> subscribedChannels = subscriptionChannelMap.get(jedisPubSub);
            BlockingQueue<List<Object>> subscriptionClientQueue = subscriptionClientQueueMap.get(client);
            for(String ch: subscribedChannels){
                List<Object> data = new ArrayList<>();
                data.add(SUBSCRIBE.getRaw());
                data.add(SafeEncoder.encode(ch));
                data.add((long) subscribedChannels.size());
                subscriptionClientQueue.put(data);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void mockClientUnsubscribe(Client client) {
        JedisPubSub jedisPubSub = subscriptionClientMap.get(client);
        List<String> subscribedChannels = subscriptionChannelMap.get(jedisPubSub);
        mockClientUnsubscribe(client, subscribedChannels.toArray(new String[0]));
    }

    private void mockClientUnsubscribe(Client client, String[] channels) {
        try {
            JedisPubSub jedisPubSub = subscriptionClientMap.get(client);
            List<String> subscribedChannels = subscriptionChannelMap.get(jedisPubSub);
            BlockingQueue<List<Object>> subscriptionClientQueue = subscriptionClientQueueMap.get(client);
            for(String ch: channels){
                subscribedChannels.remove(ch);
                List<Object> data = new ArrayList<>();
                data.add(UNSUBSCRIBE.getRaw());
                data.add(SafeEncoder.encode(ch));
                data.add((long) subscribedChannels.size());
                subscriptionClientQueue.put(data);
            }
            if (subscribedChannels.isEmpty()){
                doAfterUnflushedObjectMultiBulkReply.offer(() -> {
                    subscriptionChannelMap.remove(jedisPubSub);
                    subscriptionClientMap.remove(client);
                    subscriptionClientQueueMap.remove(client);
                });
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Long mockPublish(String channel, String message) {
        subscriptionChannelMap.forEach( (jedisPubSub, channels) -> {
            if (channels.contains(channel)) {
                //publishExecutor.execute(() -> {
                    mockPublishMessage(jedisPubSub, channel, message);
                //});
            }
        });
        return 1L;
    }

    private void mockPublishMessage(JedisPubSub jedisPubSub, String channel, String message) {
        try {
            List<Object> data = new ArrayList<>();
            data.add(MESSAGE.getRaw());
            data.add(SafeEncoder.encode(channel));
            data.add(SafeEncoder.encode(message));
            Client client = getClientFromJedisPubSub(jedisPubSub);
            BlockingQueue<List<Object>> subscriptionClientQueue = subscriptionClientQueueMap.get(client);
            subscriptionClientQueue.put(data);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Client getClientFromJedisPubSub(JedisPubSub jedisPubSub){
        Client client = null;
        for(Map.Entry<Client, JedisPubSub> entry: subscriptionClientMap.entrySet()) {
            if (entry.getValue().equals(jedisPubSub)) {
                client = entry.getKey();
                break;
            }
        }
        return client;
    }


    private void mockSubscribe(JedisPubSub jedisPubSub, List<String> channels) {
        try {
            Client client = jedis.getClient();
            subscriptionChannelMap.put(jedisPubSub, channels);
            subscriptionClientMap.put(client, jedisPubSub);
            jedisPubSub.proceed(client, channels.toArray(new String[0]));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized String mockGet(String key) {
        return data.get(key);
    }

    private synchronized Object mockEval(String script, List<String> keys, List<String> values) {
        Object response = null;
        if (script.equalsIgnoreCase(JedisLock.UNLOCK_LUA_SCRIPT)) {
            if (values.get(0).equalsIgnoreCase(data.get(keys.get(0)))){
                String removed = data.remove(keys.get(0));
                response = removed != null ? 1 : 0;
            }
        }
        return response;
    }

    private synchronized String mockSet(final String key, String value, SetParams setParams) {
        boolean insert = true;
        if (isSetParamsNX(setParams)) {
            insert = !data.containsKey(key);
        }
        if (insert) {
            data.put(key, value);
            Long expireTime = getExpireTimePX(setParams);
            if (expireTime != null){
                timer.schedule(wrapTTL(() -> data.remove(key)),expireTime);
            }
            return  CLIENT_RESPONSE_OK;
        } else {
            return  CLIENT_RESPONSE_KO;
        }
    }

    private synchronized Response<String> mockTransactionGet(String key){
        TransactionOrder<String> transactionOrder = new TransactionOrder<>(() -> mockGet(key));
        transactionActions.add(transactionOrder);
        return transactionOrder.getResponse();
    }

    private synchronized Response<String> mockTransactionSet(String key, String value, SetParams setParams){
        TransactionOrder<String> transactionOrder = new TransactionOrder<>(() -> mockSet(key, value, setParams));
        transactionActions.add(transactionOrder);
        return transactionOrder.getResponse();
    }

    private synchronized List<Object> mockTransactionExec(){
        transactionActions.forEach(TransactionOrder::execute);
        List<Object> responses = transactionActions.stream().
                map(TransactionOrder::getResponse).
                collect(Collectors.toList());
        transactionActions.clear();
        return responses;
    }


    public Jedis getJedis(){
        return jedis;
    }

    public JedisPool getJedisPool(){
        return jedisPool;
    }


    public synchronized void clearData(){
        data.clear();
        transactionActions.clear();
        subscriptionChannelMap.clear();
        subscriptionClientMap.clear();
        subscriptionClientQueueMap.clear();
        doAfterUnflushedObjectMultiBulkReply.clear();

    }

    public synchronized void close() {
        clearData();
        timer.cancel();
        if (publishExecutor instanceof java.util.concurrent.ThreadPoolExecutor) {
            ((java.util.concurrent.ThreadPoolExecutor) publishExecutor).shutdown();
        }
    }


    public synchronized Map<String,String> getCurrentData() {
        return new HashMap<>(data);
    }

    boolean isSetParamsNX(SetParams setParams) {
        boolean result = false;
        for(byte[] b: setParams.getByteParams()){
            String s = new String(b);
            if ("nx".equalsIgnoreCase(s)){
                result = true;
                break;
            }
        }
        return result;
    }

    Long getExpireTimePX(SetParams setParams) {
        return setParams.getParam("px");
    }


    // To allow deeper testing
    @SuppressWarnings("All")
    public static String getJedisLockValue(JedisLock jedisLock) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method privateMethod = JedisLock.class.getDeclaredMethod("getValue", null);
        privateMethod.setAccessible(true);
        return (String) privateMethod.invoke(jedisLock, null);
    }

}
