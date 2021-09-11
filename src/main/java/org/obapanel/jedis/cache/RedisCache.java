package org.obapanel.jedis.cache;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class RedisCache implements Cache<String,String> {
    @Override
    public String get(String key) {
        return null;
    }

    @Override
    public Map<String, String> getAll(Set<? extends String> keys) {
        return null;
    }

    @Override
    public boolean containsKey(String key) {
        return false;
    }

    @Override
    public void loadAll(Set<? extends String> keys, boolean replaceExistingValues, CompletionListener completionListener) {

    }

    @Override
    public void put(String key, String value) {

    }

    @Override
    public String getAndPut(String key, String value) {
        return null;
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> map) {

    }

    @Override
    public boolean putIfAbsent(String key, String value) {
        return false;
    }

    @Override
    public boolean remove(String key) {
        return false;
    }

    @Override
    public boolean remove(String key, String oldValue) {
        return false;
    }

    @Override
    public String getAndRemove(String key) {
        return null;
    }

    @Override
    public boolean replace(String key, String oldValue, String newValue) {
        return false;
    }

    @Override
    public boolean replace(String key, String value) {
        return false;
    }

    @Override
    public String getAndReplace(String key, String value) {
        return null;
    }

    @Override
    public void removeAll(Set<? extends String> keys) {

    }

    @Override
    public void removeAll() {

    }

    @Override
    public void clear() {

    }

    @Override
    public <C extends Configuration<String, String>> C getConfiguration(Class<C> clazz) {
        return null;
    }

    @Override
    public <T> T invoke(String key, EntryProcessor<String, String, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        return null;
    }

    @Override
    public <T> Map<String, EntryProcessorResult<T>> invokeAll(Set<? extends String> keys, EntryProcessor<String, String, T> entryProcessor, Object... arguments) {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public CacheManager getCacheManager() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return null;
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<String, String> cacheEntryListenerConfiguration) {

    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<String, String> cacheEntryListenerConfiguration) {

    }

    @Override
    public Iterator<Entry<String, String>> iterator() {
        return null;
    }
}
