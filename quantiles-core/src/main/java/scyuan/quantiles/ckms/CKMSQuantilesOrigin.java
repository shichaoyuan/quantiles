package scyuan.quantiles.ckms;

import scyuan.quantiles.Quantiles;

import java.util.*;

/**
 * Maybe Modified from: https://github.com/umbrant/QuantileEstimation/blob/master/src/main/java/com/umbrant/quantile/QuantileEstimationCKMS.java
 * Issues: The CKMS doesn't maintain the quantiles summary correctly https://github.com/umbrant/QuantileEstimation/issues/2
 *
 * Modified from: https://github.com/mayconbordin/streaminer/blob/master/src/main/java/org/streaminer/stream/quantile/CKMSQuantiles.java
 *
 * Implementation of the Cormode, Korn, Muthukrishnan, and Srivastava algorithm
 * for streaming calculation of targeted high-percentile epsilon-approximate
 * quantiles.
 *
 * This is a generalization of the earlier work by Greenwald and Khanna (GK),
 * which essentially allows different error bounds on the targeted quantiles,
 * which allows for far more efficient calculation of high-percentiles.
 *
 *
 * See: Cormode, Korn, Muthukrishnan, and Srivastava
 * "Effective Computation of Biased Quantiles over Data Streams" in ICDE 2005
 *
 * Greenwald and Khanna,
 * "Space-efficient online computation of quantile summaries" in SIGMOD 2001
 *
 * @author Jon Schneider
 */
public class CKMSQuantilesOrigin implements Quantiles {
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
    private double[] buffer = new double[500];

    private int bufferCount = 0;

    /**
     * Array of Quantiles that we care about, along with desired error.
     */
    private final Quantile quantiles[];

    private final Collection<Double> registered;

    public CKMSQuantilesOrigin(Quantile[] quantiles) {
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
        buffer[bufferCount] = value;
        bufferCount++;

        if (bufferCount == buffer.length) {
            insertBatch();
            compress();
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
        // clear the buffer
        insertBatch();
        compress();

        synchronized (sample) {
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
        }
    }

    @Override
    public Collection<Double> monitored() {
        return registered;
    }

    @Override
    public void flushBuffer() {
        return;
    }

    @Override
    public int getSampleSize() {
        return sample.size();
    }

    @Override
    public int getSize() {
        return count;
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
        if (bufferCount == 0) {
            return;
        }

        Arrays.sort(buffer, 0, bufferCount);

        synchronized (sample) {
            // Base case: no samples
            int start = 0;
            if (sample.size() == 0) {
                Item newItem = new Item(buffer[0], 1, 0);
                sample.add(newItem);
                start++;
                count++;
            }

            ListIterator<Item> it = sample.listIterator();
            Item item = it.next();

            for (int i = start; i < bufferCount; i++) {
                double v = buffer[i];
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

        bufferCount = 0;
    }

    /**
     * Try to remove extraneous items from the set of sampled items. This checks
     * if an item is unnecessary based on the desired error bounds, and merges it
     * with the adjacent item if it is.
     */
    private void compress() {
        synchronized (sample) {
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
    }

    public Quantile[] getQuantiles() {
        return quantiles;
    }

    public static Builder quantile(double quantile, double error) {
        return new Builder().quantile(quantile, error);
    }

    public static class Builder {
        List<Quantile> quantiles = new ArrayList<>();

        public Builder quantile(double quantile, double error) {
            quantiles.add(new Quantile(quantile, error));
            return this;
        }

        public CKMSQuantilesOrigin create() {
            return new CKMSQuantilesOrigin(quantiles.toArray(new Quantile[quantiles.size()]));
        }
    }
}
