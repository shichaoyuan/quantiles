package scyuan.quantiles.ckms;

import scyuan.quantiles.Quantiles;

import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 线程安全
 * 1. 使用PriorityBlockingQueue作为缓冲队列
 *
 * @author yuan.shichao
 */
public class CKMSQuantilesQueue implements Quantiles {
    /**
     * Total number of items in stream.
     */
    private int count = 0;

    /**
     * Current list of sampled items, maintained in sorted order with error bounds.
     */
    private final LinkedList<Item> sample;

    /**
     * Buffers incoming items to be inserted in batch.
     */
    private final PriorityBlockingQueue<Double> bufferQueue = new PriorityBlockingQueue<Double>();

    private final int bufferMaxSize = 200;

    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Array of Quantiles that we care about, along with desired error.
     */
    private final Quantile quantiles[];

    private final Collection<Double> registered;

    public CKMSQuantilesQueue(Quantile[] quantiles) {
        this.quantiles = quantiles;

        registered = new ArrayList<>();
        for (Quantile quantile : quantiles) {
            registered.add(quantile.getQuantile());
        }

        this.sample = new LinkedList<>();
    }

    /**
     * Add a new value from the stream.
     */
    @Override
    public void observe(double value) {
        bufferQueue.add(value);
        if (bufferQueue.size() >= bufferMaxSize) {
            if (lock.tryLock()) {
                try {
                    insertBatch();
                    compress();
                } finally {
                    lock.unlock();
                }
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
            if (sample.size() == 0) {
                return Double.NaN;
            }

            int rankMin = 0;
            int desired = (int) (q * count);

            ListIterator<Item> it = sample.listIterator();
            Item prev, cur;
            cur = it.next();
            while (it.hasNext()) {
                prev = cur;
                cur = it.next();

                rankMin += prev.g;

                if (rankMin + cur.g + cur.delta > desired + (allowableError(desired) / 2)) {
                    return prev.value;
                }
            }

            // edge case of wanting max value
            return sample.getLast().value;
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
            insertBatch();
            compress();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int getSampleSize() {
        lock.lock();
        try {
            return sample.size();
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

    /**
     * Specifies the allowable error for this rank, depending on which quantiles
     * are being targeted.
     *
     * This is the f(r_i, n) function from the CKMS paper. It's basically how wide
     * the range of this rank can be.
     *
     * @param rank the index in the list of samples
     */
    private double allowableError(int rank) {
        // NOTE: according to CKMS, this should be count, not size, but this leads
        // to error larger than the error bounds. Leaving it like this is
        // essentially a HACK, and blows up memory, but does "work".
        //int size = count;
        int size = sample.size();
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

    private void insertBatch() {
        List<Double> bufferList = new ArrayList<>();
        bufferQueue.drainTo(bufferList);
        if (bufferList.isEmpty()) {
            return;
        }

        // Base case: no samples
        int start = 0;
        if (sample.size() == 0) {
            Item newItem = new Item(bufferList.get(0), 1, 0);
            sample.add(newItem);
            start++;
            count++;
        }

        ListIterator<Item> it = sample.listIterator();
        Item item = it.next();

        for (int i = start; i < bufferList.size(); i++) {
            double v = bufferList.get(i);
            while (it.nextIndex() < sample.size() && item.value < v) {
                item = it.next();
            }

            // If we found that bigger item, back up so we insert ourselves before it
            if (item.value > v) {
                it.previous();
            }

            // We use different indexes for the edge comparisons, because of the above
            // if statement that adjusts the iterator
            int delta;
            if (it.previousIndex() == 0 || it.nextIndex() == sample.size()) {
                delta = 0;
            } else {
                delta = ((int) Math.floor(allowableError(it.nextIndex()))) - 1;
            }

            Item newItem = new Item(v, 1, delta);
            it.add(newItem);
            count++;
            item = newItem;
        }
    }

    /**
     * Try to remove extraneous items from the set of sampled items. This checks
     * if an item is unnecessary based on the desired error bounds, and merges it
     * with the adjacent item if it is.
     */
    private void compress() {
        if (sample.size() < 2) {
            return;
        }

        ListIterator<Item> it = sample.listIterator();
        Item prev;
        Item next = it.next();

        while (it.hasNext()) {
            prev = next;
            next = it.next();

            if (prev.g + next.g + next.delta <= allowableError(it.previousIndex())) {
                next.g += prev.g;
                // Remove prev. it.remove() kills the last thing returned.
                it.previous();
                it.previous();
                it.remove();
                // it.next() is now equal to next, skip it back forward again
                it.next();
            }
        }
    }

    public Quantile[] getQuantiles() {
        return quantiles;
    }

}
