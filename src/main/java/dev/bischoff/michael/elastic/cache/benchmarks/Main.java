package dev.bischoff.michael.elastic.cache.benchmarks;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class Main {
    public static void main(String[] args) throws RunnerException, IOException {
        var options = new OptionsBuilder()
                .resultFormat(org.openjdk.jmh.results.format.ResultFormatType.JSON)
                .build();

        var dir = "target/benchmarks/";

        new Runner(new OptionsBuilder().parent(options)
                .include(CacheFillBenchmark.class.getName())
                .include(CacheInvalidateAllBenchmark.class.getName())
                .include(CacheLookupBenchmark.class.getName())
                .output(dir + "jmh.out")
                .build()).run();

        Files.createDirectories(Path.of(dir));
        for(var threads : List.of(
                1,
                2,
                4,
                8
        )) {
            new Runner(new OptionsBuilder().parent(options).include(MixedPutGetBenchmarks.class.getName()).threads(threads).output(dir + "jmh-threads" + threads + ".out").result(dir + "jmh-threads" + threads + ".json").build()).run();
        }
    }
}
