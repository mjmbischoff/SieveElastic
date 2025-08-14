package dev.bischoff.michael.elastic.cache;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.ToLongBiFunction;

public class Main {

    private static final boolean PRINT_MINOR_RUN = false;

    public static void main(String[] args) {
        int[] datasetSizes = {1000, 10000, 100000, 1000000};
        System.out.println("> plain config");
        for (int size : datasetSizes) {
            Cache<String, String> lruCache = CacheBuilder.<String, String>builder().build();
            Cache<String, String> sieveCache = new SieveCache<>();
            Map<String, String> concurrentHashMap = new ConcurrentHashMap<>();
            Map<String, String> dataset = createDataset(size);
            bench(dataset, lruCache, sieveCache, concurrentHashMap);
        }
        System.out.println("> overfill 90%");
        // overfill 90%
        for (int datasetSize : datasetSizes) {
            long maxWeight = datasetSize / 10;
            ToLongBiFunction<String, String> weigher = (key, value) -> 1L;
            Cache<String, String> lruCache = CacheBuilder.<String, String>builder().setMaximumWeight(maxWeight).build();
            Cache<String, String> sieveCache = new SieveCache<>(null, maxWeight, null, weigher);
            Map<String, String> concurrentHashMap = new ConcurrentHashMap<>();
            Map<String, String> dataset = createDataset(datasetSize);
            bench(dataset, lruCache, sieveCache, concurrentHashMap);
        }
    }

    private static void bench(Map<String, String> dataset, Cache<String, String> cache, Cache<String, String> sieveCache, Map<String, String> concurrentHashMap) {
        dataset.forEach(cache::put);
        dataset.forEach(sieveCache::put);
        dataset.forEach(concurrentHashMap::put);

        List<Map.Entry<String, String>> entries = new ArrayList<>(dataset.entrySet());
        Collections.shuffle(entries);


        //warmup
        for(int i = 0; i < 100; i++) {
            for(Map.Entry<String, String> entry : entries) {
                cache.get(entry.getKey());
            }

            for(Map.Entry<String, String> entry : entries) {
                sieveCache.get(entry.getKey());
            }

            for(Map.Entry<String, String> entry : entries) {
                concurrentHashMap.get(entry.getKey());
            }
        }

        List<Long> lruCacheResults = new ArrayList<>();
        List<Long> sieveCacheResults = new ArrayList<>();
        List<Long> concurrentHashMapResults = new ArrayList<>();
        for(int i = 0; i < 50; i++) {
            if(i % 2 == 0 && PRINT_MINOR_RUN) System.out.println();
            long start = System.nanoTime();
            for(Map.Entry<String, String> entry : entries) {
                cache.get(entry.getKey());
            }
            long end = System.nanoTime();
            if(PRINT_MINOR_RUN) System.out.printf("Cache:             %10.6f ms \t", (end - start) / 1000000.0);
            lruCacheResults.add(end - start);

            start = System.nanoTime();
            for(Map.Entry<String, String> entry : entries) {
                sieveCache.get(entry.getKey());
            }
            end = System.nanoTime();
            if(PRINT_MINOR_RUN) System.out.printf("SieveCache:        %10.6f ms \t", (end - start) / 1000000.0);
            sieveCacheResults.add(end - start);

            start = System.nanoTime();
            for(Map.Entry<String, String> entry : entries) {
                concurrentHashMap.get(entry.getKey());
            }
            end = System.nanoTime();
            if(PRINT_MINOR_RUN) System.out.printf("ConcurrentHashMap: %10.6f ms \t", (end - start) / 1000000.0);
            concurrentHashMapResults.add(end - start);
        }
        System.out.println();

        float lruCacheMin = (float) lruCacheResults.stream().mapToLong(Long::longValue).min().getAsLong() / entries.size();
        float lruCacheMax = (float) lruCacheResults.stream().mapToLong(Long::longValue).max().getAsLong() / entries.size();
        float lruCacheAvg = (float) lruCacheResults.stream().mapToLong(Long::longValue).average().getAsDouble() / entries.size();
        float sieveCacheMin = (float) sieveCacheResults.stream().mapToLong(Long::longValue).min().getAsLong() / entries.size();
        float sieveCacheMax = (float) sieveCacheResults.stream().mapToLong(Long::longValue).max().getAsLong() / entries.size();
        float sieveCacheAvg = (float) sieveCacheResults.stream().mapToLong(Long::longValue).average().getAsDouble() / entries.size();
        float concurrentHashMapMin = (float) concurrentHashMapResults.stream().mapToLong(Long::longValue).min().getAsLong() / entries.size();
        float concurrentHashMapMax = (float) concurrentHashMapResults.stream().mapToLong(Long::longValue).max().getAsLong() / entries.size();
        float concurrentHashMapAvg = (float) concurrentHashMapResults.stream().mapToLong(Long::longValue).average().getAsDouble() / entries.size();

        System.out.printf( "===== dataset size: %12d =============\n", dataset.size());
        System.out.printf("lruCache min: %10.6f ns/lookup\tconcurrentHashMap min: %10.6f ns/lookup\tDifference: %10.6f\tspeedup %3.1fx\n", lruCacheMin, concurrentHashMapMin, lruCacheMin - concurrentHashMapMin, (lruCacheMin / concurrentHashMapMin));
        System.out.printf("lruCache max: %10.6f ns/lookup\tconcurrentHashMap max: %10.6f ns/lookup\tDifference: %10.6f\tspeedup %3.1fx\n", lruCacheMax, concurrentHashMapMax, lruCacheMax - concurrentHashMapMax, (lruCacheMax / concurrentHashMapMax));
        System.out.printf("lruCache avg: %10.6f ns/lookup\tconcurrentHashMap max: %10.6f ns/lookup\tDifference: %10.6f\tspeedup %3.1fx\n", lruCacheAvg, concurrentHashMapAvg, lruCacheAvg - concurrentHashMapAvg, (lruCacheAvg / concurrentHashMapAvg));
        System.out.println("----------------------------------------------");
        System.out.printf("sieveCache min: %10.6f ns/lookup\tconcurrentHashMap min: %10.6f ns/lookup\tDifference: %10.6f\tspeedup %3.1fx\n", sieveCacheMin, concurrentHashMapMin, sieveCacheMin - concurrentHashMapMin, (sieveCacheMin / concurrentHashMapMin));
        System.out.printf("sieveCache max: %10.6f ns/lookup\tconcurrentHashMap max: %10.6f ns/lookup\tDifference: %10.6f\tspeedup %3.1fx\n", sieveCacheMax, concurrentHashMapMax, sieveCacheMax - concurrentHashMapMax, (sieveCacheMax / concurrentHashMapMax));
        System.out.printf("sieveCache avg: %10.6f ns/lookup\tconcurrentHashMap max: %10.6f ns/lookup\tDifference: %10.6f\tspeedup %3.1fx\n", sieveCacheAvg, concurrentHashMapAvg, sieveCacheAvg - concurrentHashMapAvg, (sieveCacheAvg / concurrentHashMapAvg));
        System.out.println("----------------------------------------------");
        System.out.printf("lruCache min: %10.6f ns/lookup\tsieveCache min: %10.6f ns/lookup\tDifference: %10.6f\tspeedup %3.1fx\n", lruCacheMin, sieveCacheMin, lruCacheMin - sieveCacheMin, (lruCacheMin / sieveCacheMin));
        System.out.printf("lruCache max: %10.6f ns/lookup\tsieveCache max: %10.6f ns/lookup\tDifference: %10.6f\tspeedup %3.1fx\n", lruCacheMax, sieveCacheMax, lruCacheMax - sieveCacheMax, (lruCacheMax / sieveCacheMax));
        System.out.printf("lruCache avg: %10.6f ns/lookup\tsieveCache avg: %10.6f ns/lookup\tDifference: %10.6f\tspeedup %3.1fx\n", lruCacheAvg, sieveCacheAvg, lruCacheAvg - sieveCacheAvg, (lruCacheAvg / sieveCacheAvg));
        System.out.println("----------------------------------------------");

        long start = System.nanoTime();
        cache.invalidateAll();
        long end = System.nanoTime();
        System.out.printf("Cache invalidateAll:      %10.6f ms \t", (end - start) / 1000000.0);
        start = System.nanoTime();
        sieveCache.invalidateAll();
        end = System.nanoTime();
        System.out.printf("SieveCache invalidateAll: %10.6f ms \t", (end - start) / 1000000.0);
        start = System.nanoTime();
        concurrentHashMap.clear();
        end = System.nanoTime();
        System.out.printf("CHM invalidateAll:        %10.6f ms \t", (end - start) / 1000000.0);
        System.out.println();
        start = System.nanoTime();
        dataset.forEach(cache::put);
        end = System.nanoTime();
        System.out.printf("Cache fill:               %10.6f ms \t", (end - start) / 1000000.0);
        start = System.nanoTime();
        dataset.forEach(sieveCache::put);
        end = System.nanoTime();
        System.out.printf("SieveCache fill:          %10.6f ms \t", (end - start) / 1000000.0);
        start = System.nanoTime();
        dataset.forEach(concurrentHashMap::put);
        end = System.nanoTime();
        System.out.printf("ConcurrentHashMap fill:   %10.6f ms \t", (end - start) / 1000000.0);
        System.out.println();
        System.out.println("==============================================");
    }

    private static Map<String, String> createDataset(int datasetSize) {
        Map<String, String> dataset = new HashMap<>();
        for(int i = 0; i < datasetSize; i++) {
            dataset.put(randomAlphanumetic(10), randomAlphanumetic(100));
        }
        return dataset;
    }

    private static String randomAlphanumetic(int length) {
        StringBuilder sb = new StringBuilder();
        String alphanumeric = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        for (int i = 0; i < length; i++) {
            int index = (int) (Math.random() * alphanumeric.length());
            sb.append(alphanumeric.charAt(index));
        }

        return sb.toString();
    }
}