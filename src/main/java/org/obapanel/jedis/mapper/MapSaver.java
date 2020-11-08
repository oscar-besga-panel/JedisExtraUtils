package org.obapanel.jedis.mapper;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.obapanel.jedis.mapper.DataToMap.dataToMap;
import static org.obapanel.jedis.mapper.MapToData.mapToData;

public class MapSaver {

//https://gist.github.com/ningthoujam-lokhendro/fbc0ca3cf51333a230b4

    Jedis jedis;

    public MapSaver(Jedis jedis) {
        this.jedis = jedis;
    }

    public <K> void save( K data) {
        save(data.getClass() + ":" + Integer.toString(data.hashCode()));
    }

    public <K> void save(String key, K data) {
        HashMap<String, String> dataMap = dataToMap(data);
        jedis.hset(key, dataMap);
    }

    public <K> K load(String key, Class dataClass) {
        Set<String> keys = jedis.hkeys(key);
        Map<String, String> dataMap = new HashMap<>(keys.size());
        keys.forEach( k-> dataMap.put(k, jedis.hget(key, k)));
        return (K) mapToData(dataMap, dataClass);
    }

    public <K> K retrieve(String key, Class dataClass) {
        K dataResult =  load(key, dataClass);
        jedis.del(key);
        return dataResult;
    }


}
