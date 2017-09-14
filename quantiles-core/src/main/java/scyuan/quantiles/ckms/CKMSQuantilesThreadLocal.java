package scyuan.quantiles.ckms;

import org.joda.primitives.list.impl.ArrayDoubleList;
import org.joda.primitives.list.impl.ArrayIntList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scyuan.quantiles.Quantiles;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 线程安全
 * 1. 使用primitive数据类型，减少内存占用
 * 2. 按照论文重新实现 "Effective Computation of Biased Quantiles over Data Streams" in ICDE 2005
 * 3. 使用ThreadLocal buffer，减少锁争用
 *
 * @author yuan.shichao
 */
public class CKMSQuantilesThreadLocal implements Quantiles {
    private static final Logger LOGGER = LoggerFactory.getLogger(CKMSQuantilesThreadLocal.class);

    private int count = 0;

    private final ArrayDoubleList valueSample;
    private final ArrayIntList gSample;
    private final ArrayIntList deltaSample;

    private final int bufferMaxSize = 32;

    private final List<Buffer> bufferList = new CopyOnWriteArrayList<>();

    private ThreadLocal<Buffer> threadLocalBuffer = ThreadLocal.withInitial(() -> {
        Buffer buffer = new Buffer(bufferMaxSize);
        bufferList.add(buffer);
        return buffer;
    });

    private final ReentrantLock lock = new ReentrantLock();

    private final Quantile quantiles[];

    private final Collection<Double> registered;

    public CKMSQuantilesThreadLocal(Quantile[] quantiles) {
        this.quantiles = quantiles;

        registered = new ArrayList<>();
        for (Quantile quantile : quantiles) {
            registered.add(quantile.getQuantile());
        }

        valueSample = new ArrayDoubleList(bufferMaxSize);
        gSample = new ArrayIntList(bufferMaxSize);
        deltaSample = new ArrayIntList(bufferMaxSize);
    }

    @Override
    public void observe(double value) {
        Buffer buffer = threadLocalBuffer.get();
        if (buffer.addAndCheckFull(value)) {
            lock.lock();
            try {
                insertBatch(buffer);
                compress();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Get the estimated value at the specified quantile.
     *
     * @param q Queried quantile, e.g. 0.50 or 0.99.
     * @return Estimated value at that quantile.
     */
    @Override
    public double get(double q) {
        lock.lock();
        try {
            if (valueSample.size() == 0) {
                return Double.NaN;
            }

            int rankMin = 0;
            int desired = (int) (q * count);

            if (valueSample.size() == 1) {
                return valueSample.getDouble(0);
            }

            for (int i = 1; i < valueSample.size(); i++) {
                rankMin += gSample.getInt(i-1);
                if (rankMin + gSample.getInt(i) + deltaSample.getInt(i) > desired + (allowableError(desired)/2)) {
                    return valueSample.getDouble(i-1);
                }
            }

            return valueSample.getDouble(valueSample.size()-1);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Collection<Double> monitored() {
        return registered;
    }

    @Override
    public void flushBuffer() {
        lock.lock();
        try {
            for (Buffer buffer : bufferList) {
                if (buffer.count() > 0) {
                    insertBatch(buffer);
                }
            }
            bufferList.clear();
            compress();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int getSampleSize() {
        lock.lock();
        try {
            return valueSample.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int getSize() {
        lock.lock();
        try {
            return count;
        } finally {
            lock.unlock();
        }
    }

    private double allowableError(int rank) {
        int size = count;
        double minError = size + 1;

        for (Quantile q : quantiles) {
            double error;
            if (rank <= q.quantile * size) {
                error = q.u * (size - rank);
            } else {
                error = q.v * rank;
            }
            if (error < minError) {
                minError = error;
            }
        }

        return minError;
    }

    private void insertBatch(Buffer buffer) {
        double[] data = buffer.data();
        int size = buffer.count();
        Arrays.sort(data, 0, size);

        int start = 0;
        if (valueSample.size() == 0) {
            valueSample.add(data[0]);
            gSample.add(1);
            deltaSample.add(0);

            start++;
            count++;
        }

        int sampleIndex = 0;
        int curMinRank = 0;
        for (int i = start; i < size; i++) {
            double v = data[i];

            while (sampleIndex < valueSample.size() && valueSample.getDouble(sampleIndex) < v) {
                curMinRank += gSample.getInt(sampleIndex);
                sampleIndex++;
            }

            int delta;
            if (sampleIndex == 0 || sampleIndex == valueSample.size()) {
                delta = 0;
            } else {
                int ri = curMinRank - gSample.getInt(sampleIndex-1);
                delta = ((int) Math.floor(allowableError(ri))) - 1;
            }

            valueSample.add(sampleIndex, v);
            gSample.add(sampleIndex, 1);
            deltaSample.add(sampleIndex, delta);

            count++;
        }
        buffer.clear();
    }

    private void compress() {
        if (valueSample.size() < 2) {
            return;
        }

        int curMinRank = count;
        for (int i = valueSample.size() - 1; i > 0; i--) {
            int preG = gSample.getInt(i-1);
            int g = gSample.getInt(i);
            int delta = deltaSample.getInt(i);

            curMinRank -= g;
            if (preG + g + delta <= allowableError(curMinRank-preG)) {
                valueSample.removeDoubleAt(i-1);
                gSample.removeIntAt(i-1);
                deltaSample.removeIntAt(i-1);

                gSample.set(i-1, preG + g);
                curMinRank += g;
            }
        }
    }

    private static class Buffer {
        private final double[] data;
        private final int size;

        private final ReentrantLock lock = new ReentrantLock();

        private int count;

        public Buffer(int size) {
            this.size = size;
            this.data = new double[size];
            count = 0;
        }

        public boolean addAndCheckFull(double v) {
            lock.lock();
            try {
                if (count == size) {
                    LOGGER.warn("buffer is already full");
                    return true;
                }

                data[count] = v;
                count++;

                return count == size;
            } finally {
                lock.unlock();
            }
        }

        public int count() {
            lock.lock();
            try {
                return count;
            } finally {
                lock.unlock();
            }
        }

        public double[] data() {
            lock.lock();
            try {
                return data;
            } finally {
                lock.unlock();
            }
        }

        public void clear() {
            lock.lock();
            try {
                count = 0;
            } finally {
                lock.unlock();
            }
        }

    }

    public Quantile[] getQuantiles() {
        return quantiles;
    }

}
