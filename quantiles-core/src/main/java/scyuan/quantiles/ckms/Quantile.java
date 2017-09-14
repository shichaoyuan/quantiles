package scyuan.quantiles.ckms;

/**
 * @author yuan.shichao
 */
public class Quantile {
    final double quantile;
    final double error;
    final double u;
    final double v;

    public Quantile(double quantile, double error) {
        this.quantile = quantile;
        this.error = error;
        u = 2.0 * error / (1.0 - quantile);
        v = 2.0 * error / quantile;
    }

    public double getQuantile() {
        return quantile;
    }

    public double getError() {
        return error;
    }

    @Override
    public String toString() {
        return String.format("Q{q=%.3f, eps=%.3f})", quantile, error);
    }
}
