package dev.bischoff.michael.elastic.cache.benchmarks;

import dev.bischoff.michael.elastic.cache.Cache;
import dev.bischoff.michael.elastic.cache.CacheBuilder;
import dev.bischoff.michael.elastic.cache.SieveCache;
import org.openjdk.jmh.annotations.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongBiFunction;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 4)
@Warmup(iterations = 4, time = 5, timeUnit = TimeUnit.SECONDS)
@Timeout(time = 15, timeUnit = TimeUnit.SECONDS)
@Measurement(time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
public class CacheFillBenchmark {

    @Param({"1000", "10000", "100000", "1000000"})
    private int datasetSize;

    @Param({"PLAIN", "OVERFILL_10"})
    private String mode;

    private Map<String, String> dataset;
    private Cache<String, String> lruCache;
    private Cache<String, String> sieveCache;
    private Map<String, String> concurrentHashMap;

    @Setup(Level.Iteration)
    public void setup() {
        dataset = createDataset(datasetSize);

        if ("PLAIN".equals(mode)) {
            lruCache = CacheBuilder.<String, String>builder().build();
            sieveCache = new SieveCache<>();
        } else if ("OVERFILL_10".equals(mode)) {
            long maxWeight = datasetSize / 10;
            ToLongBiFunction<String, String> weigher = (key, value) -> 1L;
            lruCache = CacheBuilder.<String, String>builder().setMaximumWeight(maxWeight).build();
            sieveCache = new SieveCache<>(null, maxWeight, null, weigher);
        }

        concurrentHashMap = new ConcurrentHashMap<>();
    }

    // --- Fill benchmarks ---
    @Benchmark
    public void lruCacheFill() {
        dataset.forEach(lruCache::put);
    }

    @Benchmark
    public void sieveCacheFill() {
        dataset.forEach(sieveCache::put);
    }

    @Benchmark
    public void chmFill() {
        dataset.forEach(concurrentHashMap::put);
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
