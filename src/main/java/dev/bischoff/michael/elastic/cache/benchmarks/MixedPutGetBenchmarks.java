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

@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 4)
@Warmup(iterations = 4, time = 5, timeUnit = TimeUnit.SECONDS)
@Timeout(time = 15, timeUnit = TimeUnit.SECONDS)
@Measurement(time = 5, timeUnit = TimeUnit.SECONDS)
public class MixedPutGetBenchmarks {

    public enum CacheType {
        LRU(() -> CacheBuilder.<String, String>builder().build()),
        SIEVE(SieveCache::new),
        CHM(ConcurrentHashMapWrapper::new);

        private final Supplier<Cache<String, String>> supplier;
        CacheType(Supplier<Cache<String, String>> supplier) { this.supplier = supplier; }
        Cache<String, String> create() { return supplier.get(); }
    }

    @State(Scope.Benchmark)
    public static class BaseState {
        @Param({"LRU", "SIEVE", "CHM"})
        public String cacheTypeName;

        protected List<Map.Entry<String,String>> hotEntries;   // likely hits
        protected List<Map.Entry<String,String>> coldEntries;  // likely misses
        protected Map<String, String> dataset;

        protected Random random;
        protected Cache<String, String> cache;

        private final int datasetSize;
        private final int hotRatio; // percentage of keys that are hot

        public BaseState() {
            datasetSize = 0;
            hotRatio = 0;
        }

        protected BaseState(int datasetSize, int hotRatio) {
            this.datasetSize = datasetSize;
            this.hotRatio = hotRatio;
        }

        @Setup(Level.Trial)
        public void setupBenchmark() {
            Random random = new Random(12345);

            // Create dataset
            dataset = createDataset(datasetSize);
            hotEntries = new ArrayList<>(dataset.entrySet());
            Collections.shuffle(hotEntries, random);

            // Cold keys (never seen before)
            coldEntries = new ArrayList<>();
            for (int i = 0; i < datasetSize; i++) {
                coldEntries.add(new AbstractMap.SimpleEntry<>(randomString(random, 10), randomString(random, 100)));
            }
        }

        @Setup(Level.Iteration)
        public void setupIteration() {
            random = new Random(12345);
            cache = CacheType.valueOf(cacheTypeName).create();
            dataset.forEach(cache::put); // Hot set in cache
        }

        public Map.Entry<String, String> nextEntry() {
            if (random.nextInt(100) < hotRatio) {
                return hotEntries.get(random.nextInt(hotEntries.size()));
            } else {
                return coldEntries.get(random.nextInt(coldEntries.size()));
            }
        }

        private static Map<String, String> createDataset(int datasetSize) {
            Map<String, String> dataset = new HashMap<>();
            Random r = new Random(42);
            for (int i = 0; i < datasetSize; i++) {
                dataset.put(randomString(r, 10), randomString(r, 100));
            }
            return dataset;
        }

        private static String randomString(Random r, int length) {
            String alphanumeric = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            StringBuilder sb = new StringBuilder(length);
            for (int i = 0; i < length; i++) {
                sb.append(alphanumeric.charAt(r.nextInt(alphanumeric.length())));
            }
            return sb.toString();
        }
    }

    public static class State100 extends BaseState { public State100() { super(100, 90); } }
    public static class State1k extends BaseState { public State1k() { super(1_000, 90); } }
    public static class State10k extends BaseState { public State10k() { super(10_000, 90); } }

    @Benchmark
    public void mixedPutGet1_100(State100 s, Blackhole bh) {
        var entry = s.nextEntry();
        String value = s.cache.get(entry.getKey());
        if (value == null) {
            s.cache.put(entry.getKey(), entry.getValue());
        }
        bh.consume(value);
    }

    @Benchmark
    public void mixedPutGet2_1k(State1k s, Blackhole bh) {
        var entry = s.nextEntry();
        String value = s.cache.get(entry.getKey());
        if (value == null) {
            s.cache.put(entry.getKey(), entry.getValue());
        }
        bh.consume(value);
    }

    @Benchmark
    public void mixedPutGet3_10k(State10k s, Blackhole bh) {
        var entry = s.nextEntry();
        String value = s.cache.get(entry.getKey());
        if (value == null) {
            s.cache.put(entry.getKey(), entry.getValue());
        }
        bh.consume(value);
    }

}
