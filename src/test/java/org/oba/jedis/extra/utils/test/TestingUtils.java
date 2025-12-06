package org.oba.jedis.extra.utils.test;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.params.SetParams;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class TestingUtils {

    private TestingUtils() {}

    public static boolean isSetParamsNX(SetParams setParams) {
        if (setParams != null) {
            Protocol.Keyword valueExistance = extractPrivateValue("existance", SetParams.class, setParams, Protocol.Keyword.class);
            return valueExistance == Protocol.Keyword.NX;
        } else {
            return false;
        }
    }

    public static Long extractSetParamsExpireTimePX(SetParams setParams) {
        if (setParams != null) {
            Protocol.Keyword valueExpiration = extractPrivateValue("expiration", SetParams.class, setParams, Protocol.Keyword.class, true);
            Long valueExpirationValue = extractPrivateValue("expirationValue", SetParams.class, setParams, Long.class, true);
            if (valueExpiration == null && valueExpirationValue == null) {
                return null;
            } else if (valueExpiration == Protocol.Keyword.PX) {
                return valueExpirationValue;
            } else {
                throw new IllegalArgumentException("SetParams is not PX " + setParams);
            }
        } else {
            return null;
        }

    }


    public static <I,O> O extractPrivateValue(String field, Class<I> originType, I origin, Class<O> resultType) {
        return extractPrivateValue(field, originType, origin, resultType, false);
    }

    public static <I,O> O extractPrivateValue(String field, Class<I> originType, I origin, Class<O> resultType, boolean searchInParent) {
        try {
            Field privateField;
            if (searchInParent) {
                privateField = getPrivateFieldsFromObjectAndParent(originType, field);
            } else {
                privateField = origin.getClass().getDeclaredField(field);
            }
            privateField.setAccessible(true);
            if (resultType == privateField.getType()){
                return (O) privateField.get(origin);
            } else {
                String error = String.format("Error in field %s originType %s origin %s resultType %s", field, originType,
                        origin, resultType);
                throw new RuntimeException(error);
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            String error = String.format("Error in field %s originType %s origin %s resultType %s", field, originType,
                    origin, resultType);
            throw new RuntimeException(error, e);
        }
    }

    public static Field getPrivateFieldsFromObjectAndParent(Class<?> type, String name) {
        List<Field> fieldList = new ArrayList<>();
        Class<?> currentType = type;
        while (currentType != null && currentType != Object.class) {
            for (Field field : currentType.getDeclaredFields()) {
                if (!field.isSynthetic()) {
                    fieldList.add(field);
                }
            }
            currentType = currentType.getSuperclass();
        }
        return fieldList.stream().
                filter(f -> f.getName().equals(name)).
                findFirst().get();

    }

}
