package dev.bischoff.michael.elastic.cache.benchmarks;

import dev.bischoff.michael.elastic.cache.Cache;
import dev.bischoff.michael.elastic.cache.CacheBuilder;
import dev.bischoff.michael.elastic.cache.ConcurrentHashMapWrapper;
import dev.bischoff.michael.elastic.cache.SieveCache;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 4)
@Warmup(iterations = 4, time = 5, timeUnit = TimeUnit.SECONDS)
@Timeout(time = 15, timeUnit = TimeUnit.SECONDS)
@Measurement(time = 5, timeUnit = TimeUnit.SECONDS)
public class CacheLookupBenchmark {

    public enum CacheType {
        LRU(() -> CacheBuilder.<String, String>builder().build()),
        SIEVE(SieveCache::new),
        CHM(ConcurrentHashMapWrapper::new);

        private final Supplier<Cache<String, String>> supplier;

        CacheType(Supplier<Cache<String, String>> supplier) {
            this.supplier = supplier;
        }

        Cache<String, String> create() {
            return supplier.get();
        }
    }

    @State(Scope.Thread)
    public static class BaseState {
        @Param({"LRU", "SIEVE", "CHM"})
        public String cacheTypeName;

        protected Cache<String, String> cache;
        protected List<String> keys;
        private final int datasetSize;

        public BaseState() {
            datasetSize = 0;
        }

        protected BaseState(int datasetSize) {
            this.datasetSize = datasetSize;
        }

        @Setup(Level.Trial)
        public void setup() {
            Map<String, String> dataset = createDataset(datasetSize);
            keys = new ArrayList<>(dataset.keySet());
            Collections.shuffle(keys);

            cache = CacheType.valueOf(cacheTypeName).create();
            dataset.forEach(cache::put);
        }

        private static Map<String, String> createDataset(int datasetSize) {
            Map<String, String> dataset = new HashMap<>();
            Random r = new Random(12345);
            String alphanumeric = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            for (int i = 0; i < datasetSize; i++) {
                dataset.put(randomString(r, alphanumeric, 10), randomString(r, alphanumeric, 100));
            }
            return dataset;
        }

        private static String randomString(Random r, String chars, int length) {
            StringBuilder sb = new StringBuilder(length);
            for (int i = 0; i < length; i++) {
                sb.append(chars.charAt(r.nextInt(chars.length())));
            }
            return sb.toString();
        }
    }

    public static class State1k extends BaseState { public State1k() { super(1_000); } }
    public static class State10k extends BaseState { public State10k() { super(10_000); } }
    public static class State100k extends BaseState { public State100k() { super(100_000); } }
    public static class State1M extends BaseState { public State1M() { super(1_000_000); } }

    @Benchmark @OperationsPerInvocation(1_000)
    public void lookup_1k(State1k s, Blackhole bh) {
        for (String key : s.keys) {
            bh.consume(s.cache.get(key));
        }
    }

    @Benchmark @OperationsPerInvocation(10_000)
    public void lookup_10k(State10k s, Blackhole bh) {
        for (String key : s.keys) {
            bh.consume(s.cache.get(key));
        }
    }

    @Benchmark @OperationsPerInvocation(100_000)
    public void lookup_100k(State100k s, Blackhole bh) {
        for (String key : s.keys) {
            bh.consume(s.cache.get(key));
        }
    }

    @Benchmark @OperationsPerInvocation(1_000_000)
    public void lookup_1M(State1M s, Blackhole bh) {
        for (String key : s.keys) {
            bh.consume(s.cache.get(key));
        }
    }
}
