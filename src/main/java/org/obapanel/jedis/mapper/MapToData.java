package org.obapanel.jedis.mapper;

import redis.clients.jedis.exceptions.JedisException;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public class MapToData {



    public static <K> K mapToData(Map<String, String> data, Class dataClass) {
        try {
            K dataObject = (K) dataClass.newInstance();
            BeanInfo beanInfo = Introspector.getBeanInfo(dataClass );
            for(PropertyDescriptor pd : beanInfo.getPropertyDescriptors()) {
                Class propertyClass = pd.getPropertyType();
                String propertyData = data.get(pd.getName());
                Method writer = pd.getWriteMethod();
                if (propertyClass == String.class) {
                    writer.invoke(dataObject, propertyData);
                } else if (propertyClass == Boolean.class || propertyClass == Boolean.TYPE) {
                    writer.invoke(dataObject, Boolean.parseBoolean(propertyData));
                } else if (propertyClass == Character.class || propertyClass == Character.TYPE) {
                    writer.invoke(dataObject, propertyData.charAt(0));
                } else if (propertyClass == Byte.class || propertyClass == Byte.TYPE) {
                    writer.invoke(dataObject, Byte.parseByte(propertyData));
                } else if (propertyClass == Short.class || propertyClass == Short.TYPE) {
                    writer.invoke(dataObject, Short.parseShort(propertyData));
                } else if (propertyClass == Integer.class || propertyClass == Integer.TYPE) {
                    writer.invoke(dataObject, Integer.parseInt(propertyData));
                } else if (propertyClass == Long.class || propertyClass == Long.TYPE) {
                    writer.invoke(dataObject, Long.parseLong(propertyData));
                } else if (propertyClass == Float.class || propertyClass == Float.TYPE) {
                    writer.invoke(dataObject, Float.parseFloat(propertyData));
                } else if (propertyClass == Double.class || propertyClass == Double.TYPE) {
                    writer.invoke(dataObject, Double.parseDouble(propertyData));
                }
            }
            return dataObject;
        } catch (IntrospectionException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new JedisException("Error while passing data to map", e);
        }
    }

}
