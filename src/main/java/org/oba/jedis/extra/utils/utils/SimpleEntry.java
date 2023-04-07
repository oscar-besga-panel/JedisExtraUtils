package org.oba.jedis.extra.utils.utils;

import java.util.Map;
import java.util.Objects;

/**
 * A map.entry implementation
 * Immutable, as setValue is not implemented
 */
public class SimpleEntry implements Map.Entry<String, String> {

    private final String key;
    private final String value;

    /**
     * Create a simple entry
     * @param key key data
     * @param value value data
     */
    public SimpleEntry(String key, String value) {
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
