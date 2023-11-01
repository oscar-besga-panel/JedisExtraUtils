package org.oba.jedis.extra.utils.test;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.params.SetParams;

import java.lang.reflect.Field;

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
            Protocol.Keyword valueExpiration = extractPrivateValue("expiration", SetParams.class, setParams, Protocol.Keyword.class);
            Long valueExpirationValue = extractPrivateValue("expirationValue", SetParams.class, setParams, Long.class);
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
        try {
            Field privateField = origin.getClass().getDeclaredField(field);
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

}
