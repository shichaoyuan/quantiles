package scyuan.quantiles.ckms;

/**
 * @author yuan.shichao
 */
public class Item {
    public final double value;
    int g;
    final int delta;

    Item(double value, int lower_delta, int delta) {
        this.value = value;
        this.g = lower_delta;
        this.delta = delta;
    }

    @Override
    public String toString() {
        return String.format("%4.3f, %d, %d", value, g, delta);
    }
}
