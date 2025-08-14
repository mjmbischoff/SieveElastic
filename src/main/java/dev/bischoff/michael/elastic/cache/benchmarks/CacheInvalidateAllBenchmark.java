package dev.bischoff.michael.elastic.cache.benchmarks;

import dev.bischoff.michael.elastic.cache.Cache;
import dev.bischoff.michael.elastic.cache.CacheBuilder;
import dev.bischoff.michael.elastic.cache.SieveCache;
import org.openjdk.jmh.annotations.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class CacheInvalidateAllBenchmark {

    @Param({"100", "1000", "10000", "100000"})
    private int datasetSize;

    private Map<String, String> dataset;
    private Cache<String, String> lruCache;
    private Cache<String, String> sieveCache;
    private Map<String, String> concurrentHashMap;

    @Setup(Level.Iteration)
    public void setup() {
        dataset = createDataset(datasetSize);

        lruCache = CacheBuilder.<String, String>builder().build();
        sieveCache = new SieveCache<>();
        concurrentHashMap = new ConcurrentHashMap<>();

        // Fill once so that clearing benchmarks have something to clear
        dataset.forEach(lruCache::put);
        dataset.forEach(sieveCache::put);
        dataset.forEach(concurrentHashMap::put);
    }

    @Benchmark
    public void lruCacheClear() {
        lruCache.invalidateAll();
    }

    @Benchmark
    public void sieveCacheClear() {
        sieveCache.invalidateAll();
    }

    @Benchmark
    public void chmClear() {
        concurrentHashMap.clear();
    }

    private static Map<String, String> createDataset(int datasetSize) {
        Random random = new Random(12345);
        Map<String, String> dataset = new HashMap<>();
        for (int i = 0; i < datasetSize; i++) {
            dataset.put(randomAlphanumeric(10, random), randomAlphanumeric(100, random));
        }
        return dataset;
    }

    private static String randomAlphanumeric(int length, Random random) {
        StringBuilder sb = new StringBuilder();
        String alphanumeric = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        // fixed seed for reproducibility
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(alphanumeric.length());
            sb.append(alphanumeric.charAt(index));
        }
        return sb.toString();
    }
}
