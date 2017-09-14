package scyuan.quantiles;

import org.openjdk.jmh.annotations.*;
import scyuan.quantiles.ckms.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @author yuan.shichao
 */

@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.MINUTES)
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.MINUTES)
@Fork(5)
@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
public class QuantilesTestBenchmark {

    public static class BaseEstimator {
        private Quantiles estimator;
        private ScheduledExecutorService executor;

        public Quantiles get() {
            return estimator;
        }

        protected void setup(String type) {
            Quantile[] quantiles = new Quantile[] {
                    new Quantile(0.50, 0.01),
                    new Quantile(0.90, 0.01),
                    new Quantile(0.95, 0.001),
                    new Quantile(0.99, 0.001),
                    new Quantile(0.999, 0.0001),
                    new Quantile(0.9999, 0.00001)};

            switch (type) {
                case "mt":
                    estimator = new CKMSQuantilesMT(quantiles);
                    break;
                case "queue":
                    estimator = new CKMSQuantilesQueue(quantiles);
                    break;
                case "primitive":
                    estimator = new CKMSQuantilesPrimitive(quantiles);
                    break;
                case "threadlocal":
                    estimator = new CKMSQuantilesThreadLocal(quantiles);
                    break;
                default:
                    estimator = new CKMSQuantilesMT(quantiles);
            }

            executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(() -> {
                estimator.flushBuffer();
                System.out.println();
                System.out.println("[Stat] # of samples: " + estimator.getSampleSize());
                System.out.println("[Stat] # of data: " + estimator.getSize());
                System.out.println();
            }, 1, 1, TimeUnit.MINUTES);
        }

        protected void teardown() {
            executor.shutdown();
        }
    }

    @State(Scope.Benchmark)
    public static class EstimatorMT extends BaseEstimator {
        @Setup
        public void setup() {
            super.setup("mt");
        }

        @TearDown
        public void teardown() {
            super.teardown();
        }

    }

    @State(Scope.Benchmark)
    public static class EstimatorQueue extends BaseEstimator {
        @Setup
        public void setup() {
            super.setup("queue");
        }

        @TearDown
        public void teardown() {
            super.teardown();
        }

    }

    @State(Scope.Benchmark)
    public static class EstimatorPrimitive extends BaseEstimator {
        @Setup
        public void setup() {
            super.setup("primitive");
        }

        @TearDown
        public void teardown() {
            super.teardown();
        }

    }

    @State(Scope.Benchmark)
    public static class EstimatorThreadLocal extends BaseEstimator {
        @Setup
        public void setup() {
            super.setup("threadlocal");
        }

        @TearDown
        public void teardown() {
            super.teardown();
        }

    }

    @Benchmark
    public double mt(EstimatorMT estimator) {
        double value = ThreadLocalRandom.current().nextDouble(10000000000d);
        estimator.get().observe(value);
        return value;
    }

    @Benchmark
    public double queue(EstimatorQueue estimator) {
        double value = ThreadLocalRandom.current().nextDouble(10000000000d);
        estimator.get().observe(value);
        return value;
    }

    @Benchmark
    public double primitive(EstimatorPrimitive estimator) {
        double value = ThreadLocalRandom.current().nextDouble(10000000000d);
        estimator.get().observe(value);
        return value;
    }

    @Benchmark
    public double threadlocal(EstimatorThreadLocal estimator) {
        double value = ThreadLocalRandom.current().nextDouble(10000000000d);
        estimator.get().observe(value);
        return value;
    }
}
