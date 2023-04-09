package org.oba.jedis.extra.utils.collections;

import org.mockito.Mockito;
import org.oba.jedis.extra.utils.test.TTL;
import org.oba.jedis.extra.utils.test.TransactionOrder;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBase;
import redis.clients.jedis.args.ListPosition;
import redis.clients.jedis.params.SetParams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Mock of jedis methods used by the lock
 */
public class MockOfJedisForList {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockOfJedisForList.class);

    public static final String CLIENT_RESPONSE_OK = "OK";
    public static final String CLIENT_RESPONSE_KO = "KO";


    // Zero to prevent some unit test
    // One to one pass
    // More to more passes
    static final int UNIT_TEST_CYCLES_LIST = 1;

    static boolean unitTestEnabledForList(){
        return UNIT_TEST_CYCLES_LIST > 0;
    }

    private final Jedis jedis;
    private final JedisPool jedisPool;
    private final Map<String, Object> data = Collections.synchronizedMap(new HashMap<>());
    private final Timer timer;


    public MockOfJedisForList() {
        PowerMockito.suppress(MemberMatcher.methodsDeclaredIn(TransactionBase.class));

        timer = new Timer();

        jedis = Mockito.mock(Jedis.class);
        jedisPool = Mockito.mock(JedisPool.class);
        when(jedisPool.getResource()).thenReturn(jedis);

        Transaction transaction = PowerMockito.mock(Transaction.class);

        when(jedis.multi()).thenReturn(transaction);
        when(jedis.exists(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockExists(key);
        });
        when(jedis.del(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockDel(key);
        });
        when(jedis.get(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return (String)mockGet(key);
        });
        when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            SetParams setParams = ioc.getArgument(2);
            return mockSet(key, value, setParams);
        });

        when(jedis.llen(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockListLlen(key);
        });
        when(jedis.lrange(anyString(), anyLong(), anyLong())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            long from = ioc.getArgument(1);
            long to = ioc.getArgument(2);
            return mockListLrange(key, from, to);
        });
        when(jedis.rpush(anyString(), any())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String[] data = null;
            if (ioc.getArguments().length > 1) {
                List<String> tmp = new ArrayList<>();
                for(int i = 1; i < ioc.getArguments().length; i++){
                    tmp.add(ioc.getArgument(i));
                }
                data = tmp.toArray(new String[0]);
            } else {
                Object o = ioc.getArgument(1);
                if (o instanceof String[]) {
                    data = (String[]) o;
                } else {
                    data = new String[]{o.toString()};
                }
            }
            return mockListRpush(key, data);
        });
        when(jedis.lindex(anyString(), anyLong())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            long index = ioc.getArgument(1);
            return mockListLindex(key, index);
        });
        when(jedis.lrem(anyString(), anyLong(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            long count = ioc.getArgument(1);
            String value = ioc.getArgument(2);
            return mockListLlrem(key, count, value);
        });
        when(jedis.linsert(anyString(), any(ListPosition.class), anyString(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            ListPosition listPosition = ioc.getArgument(1);
            String pivot = ioc.getArgument(2);
            String argument = ioc.getArgument(3);
            return mockListLlinsert(key, listPosition, pivot, argument);
        });
        when(jedis.eval(anyString(),any(List.class), any(List.class))).thenAnswer(ioc -> {
            String script = ioc.getArgument(0);
            List<String> keys = ioc.getArgument(1);
            List<String> values = ioc.getArgument(2);
            return mockEval(script, keys, values);
        });
        when(transaction.lindex(anyString(), anyLong())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            long index = ioc.getArgument(1);
            return TransactionOrder.quickReponseExecuted(mockListLindex(key, index));
        });
        when(transaction.lset(anyString(), anyLong(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            long index = ioc.getArgument(1);
            String value = ioc.getArgument(2);
            return TransactionOrder.quickReponseExecuted(mockListLset(key, index, value));
        });
        when(transaction.lrem(anyString(), anyLong(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            long count = ioc.getArgument(1);
            String value = ioc.getArgument(2);
            return TransactionOrder.quickReponseExecuted(mockListLlrem(key, count, value));
        });
        PowerMockito.when(transaction.exec()).thenAnswer(ioc -> mockTransactionExec());

    }



    Jedis getJedis(){
        return jedis;
    }

    JedisPool getJedisPool() {
        return jedisPool;
    }



    synchronized boolean mockExists(String key) {
        return data.containsKey(key);
    }

    synchronized Object mockGet(String key) {
        return data.get(key);
    }

    synchronized String mockSet(final String key, String value, SetParams setParams) {
        boolean insert = true;
        if (isSetParamsNX(setParams)) {
            insert = !data.containsKey(key);
        }
        if (insert) {
            data.put(key, value);
            Long expireTime = getExpireTimePX(setParams);
            if (expireTime != null){
                timer.schedule(TTL.wrapTTL(() -> data.remove(key)),expireTime);
            }
            return  CLIENT_RESPONSE_OK;
        } else {
            return  CLIENT_RESPONSE_KO;
        }
    }

    synchronized Long mockDel(String key) {
        if (data.containsKey(key)) {
            data.remove(key);
            return 1L;
        } else {
            return 0L;
        }
    }



    synchronized void clearData(){
        data.clear();
    }


    synchronized Map<String,Object> getCurrentData() {
        return new HashMap<>(data);
    }

    synchronized void put(String key, Object element) {
        data.put(key, element);
    }

    boolean isSetParamsNX(SetParams setParams) {
        boolean result = false;
        for(byte[] b: setParams.getByteParams()){
            String s = new String(b);
            if ("nx".equalsIgnoreCase(s)){
                result = true;
            }
        }
        return result;
    }

    Long getExpireTimePX(SetParams setParams) {
        return (Long) setParams.getParam("px");
    }

    synchronized ArrayList<String> dataToList(String key) {
        return dataToList(key, false);
    }

    synchronized ArrayList<String> dataToList(String key, boolean createIfNoExists) {
        if (data.containsKey(key)) {
            Object o = data.get(key);
            if (!(o instanceof ArrayList)) throw new IllegalStateException("MockOfJedis not arraylist data of key " + key);
            return (ArrayList<String>) o;
        } else if (createIfNoExists) {
            ArrayList<String> list = new ArrayList<>();
            data.put(key, list);
            return list;
        } else {
            return new ArrayList<>(0);
        }

    }

    synchronized long mockListLlen(String key) {
        return dataToList(key).size();
    }




    synchronized ArrayList<String> mockListLrange(String key, long from, long to) {
        ArrayList<String> data = dataToList(key);
        if (from == 0 && to == -1) {
            return data;
        } else {
            return new ArrayList<>(data.subList(Long.valueOf(from).intValue() , Long.valueOf(to + 1).intValue()));
        }
    }

    synchronized long mockListRpush(String key, String[] dataToAdd) {
        ArrayList<String> list = dataToList(key, true);
        for(String s: dataToAdd){
            list.add(s);
        }
        return list.size();
    }

    synchronized String mockListLindex(String key, long index) {
        return dataToList(key).get(Long.valueOf(index).intValue());
    }

    synchronized Long mockListLlrem(String key, long count, String value) {
        List<String> list = dataToList(key);
        long numOfDeletes = 0;
        for(long i=0; i < count; i++){
            boolean removed = list.remove(value);
            if (removed) {
                numOfDeletes++;
            }
        }
        return numOfDeletes;
    }

    synchronized Object mockListLlinsert(String key, ListPosition listPosition, String pivot, String element) {
        ArrayList<String> data = dataToList(key);
        long inserted = 0L;
        int pos = data.indexOf(pivot);
        if (pos < 0 || pos >= data.size()) {
            // NOOP
        } else if (ListPosition.BEFORE.equals(listPosition)) {
            data.add(pos,element);
            inserted++;
        } else if (ListPosition.AFTER.equals(listPosition)) {
            pos++;
            if (pos >= data.size()) {
                data.add(element);
                inserted++;
            } else {
                data.add(pos, element);
                inserted++;
            }
        }
        return inserted;
    }

    Object mockListLset(String key, long index, String value) {
        ArrayList<String> data = dataToList(key);
        data.set(Long.valueOf(index).intValue(), value);
        return CLIENT_RESPONSE_OK;
    }

    synchronized Object mockEval(String script, List<String> keys, List<String> values) {
        Object response = null;
        if (script.equalsIgnoreCase(JedisList.LUA_SCRIPT_INDEX_OF)) {
            ArrayList<String> data = dataToList(keys.get(0));
            response = (long)data.indexOf(values.get(0));
        } else if (script.equalsIgnoreCase(JedisList.LUA_SCRIPT_LAST_INDEX_OF)) {
            ArrayList<String> data = dataToList(keys.get(0));
            response = (long)data.lastIndexOf(values.get(0));
        }
        return response;
    }

    private Object mockTransactionExec() {
        LOGGER.debug("mockTransactionExec do nothing");
        return new ArrayList<Object>(0);
    }



}
