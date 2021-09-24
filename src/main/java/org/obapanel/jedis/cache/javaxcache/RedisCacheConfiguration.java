package org.obapanel.jedis.cache.javaxcache;

import javax.cache.configuration.Configuration;

public class RedisCacheConfiguration implements Configuration<String, String> {

    @Override
    public Class<String> getKeyType() {
        return String.class;
    }

    @Override
    public Class<String> getValueType() {
        return String.class;
    }

    @Override
    public boolean isStoreByValue() {
        return false;
    }
}
