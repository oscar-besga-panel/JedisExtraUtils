package org.obapanel.jedis.cache.javaxcache;

import javax.cache.Cache;
import java.util.Map;
import java.util.Objects;

class SimpleEntry implements Cache.Entry<String, String>, Map.Entry<String, String> {

    private String key;
    private String value;

    SimpleEntry(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String setValue(String value) {
        throw new UnsupportedOperationException("Not supported here");
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return RedisCacheUtils.unwrap(clazz, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimpleEntry)) return false;
        SimpleEntry that = (SimpleEntry) o;
        return Objects.equals(getKey(), that.getKey()) && Objects.equals(getValue(), that.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getKey(), getValue());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SimpleEntry{");
        sb.append("key='").append(key).append('\'');
        sb.append(", value='").append(value).append('\'');
        sb.append('}');
        return sb.toString();
    }

}
