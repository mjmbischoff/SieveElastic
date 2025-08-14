package dev.bischoff.michael.elastic.cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

/**
 * Wrapper to adapt ConcurrentHashMap to Cache interface.
 */
public class ConcurrentHashMapWrapper<K, V> implements Cache<K, V> {
    private final ConcurrentHashMap<K, V> map = new ConcurrentHashMap<>();

    @Override
    public V get(K k) {
        return map.get(k);
    }

    @Override
    public void put(K k, V v) {
        map.put(k, v);
    }

    @Override
    public V computeIfAbsent(K k, CacheLoader<K, V> loader) throws ExecutionException {
        return map.computeIfAbsent(k, k1 -> {
            try {
                return loader.load(k);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void invalidate(K k) {
        map.remove(k);
    }

    @Override
    public void invalidate(K k, V v) {
        map.remove(k, v);
    }

    @Override
    public void invalidateAll() {
        map.clear();
    }

    @Override
    public int count() {
        return map.size();
    }

    @Override
    public long weight() {
        return 0;
    }

    @Override
    public Iterable<K> keys() {
        return map.keySet();
    }

    @Override
    public Iterable<V> values() {
        return map.values();
    }

    @Override
    public Stats stats() {
        return null;
    }

    @Override
    public void forEach(BiConsumer<K, V> consumer) {
        map.forEach(consumer);
    }
}
