package org.obapanel.jedis.cache.javaxcache;

public final class RedisCacheUtils {

    public static <T> T unwrap(Class<T> clazz, Object source) {
        if (clazz == null) throw new NullPointerException("unwrap clazz is null");
        if (clazz.isAssignableFrom(source.getClass())){
            return clazz.cast(source);
        } else {
            throw new IllegalArgumentException("Class " + clazz + " is not assinable from " + source.getClass());
        }
    }
}
