package scyuan.quantiles;

import java.util.Collection;

/**
 * @author yuan.shichao
 */
public interface Quantiles {

    /**
     * 添加数据
     *
     * @param value
     */
    void observe(double value);

    /**
     * 获取百分位数
     *
     * @param percentile (0 .. 1)
     * @return
     */
    double get(double percentile);


    /**
     * 清空缓存中的数据
     */
    void flushBuffer();

    /**
     * 获取所有百分位
     *
     * @return
     */
    Collection<Double> monitored();

    /**
     * 获取采样数据个数
     *
     * @return
     */
    int getSampleSize();

    /**
     * 获取数据总个数
     *
     * @return
     */
    int getSize();
}
