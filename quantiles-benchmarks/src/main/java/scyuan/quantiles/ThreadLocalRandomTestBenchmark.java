package scyuan.quantiles;

import org.openjdk.jmh.annotations.*;
import scyuan.quantiles.ckms.CKMSQuantilesMT;
import scyuan.quantiles.ckms.Quantile;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @author yuan.shichao
 */

@Warmup(iterations = 1, time = 30, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 1, timeUnit = TimeUnit.MINUTES)
@Fork(1)
@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
public class ThreadLocalRandomTestBenchmark {


    @Benchmark
    public double update() {
        double value = ThreadLocalRandom.current().nextDouble(10000000000d);
        return value;
    }

}
