package org.obapanel.jedis.mapper;

import redis.clients.jedis.exceptions.JedisException;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;

public class DataToMap {


    public static <K> HashMap<String, String> dataToMap(K data) {
        try {
            HashMap<String, String> hashMap = new HashMap<>();
            BeanInfo beanInfo = Introspector.getBeanInfo(data.getClass());
            for(PropertyDescriptor pd : beanInfo.getPropertyDescriptors()) {
                Method reader = pd.getReadMethod();
                if( reader != null  && data != null) {
                    Object property = reader.invoke(data);
                    if (property != null) {
                        hashMap.put(pd.getName(), "" + property);
                    }
                }
            }
            return hashMap;

        } catch (IntrospectionException | IllegalAccessException | InvocationTargetException e) {
            throw new JedisException("Error while passing data to map", e);
        }
    }
}
