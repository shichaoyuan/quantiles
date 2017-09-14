package scyuan.quantiles;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import scyuan.quantiles.ckms.*;

import java.util.*;

/**
 * @author yuan.shichao
 */
public class CKMSQuantilesTest {

    private static int size = 1000;
    private static boolean isRand = false;
    private static Quantile[] quantiles;

    private static double[] dataArray;

    @BeforeClass
    public static void setup() {
        size = 10000000;
        isRand = false;
//        isRand = true;

        quantiles = new Quantile[] {
                new Quantile(0.50, 0.01),
                new Quantile(0.90, 0.01),
                new Quantile(0.95, 0.001),
                new Quantile(0.99, 0.001),
                new Quantile(0.999, 0.0001),
                new Quantile(0.9999, 0.00001)
        };

        dataArray = new double[size];
        Random r = new Random();
        for (int i = 0; i < size; i++) {
            if (isRand) {
                dataArray[i] = r.nextInt(size);
            } else {
                dataArray[i] = i;
            }
        }
        if (!isRand) {
            shuffle(dataArray, r);
        }
    }

    @Test
    public void testOrigin() {
        Quantiles estimator = new CKMSQuantilesOrigin(quantiles);
        estimate(estimator);
    }

    @Test
    public void testMT() {
        Quantiles estimator = new CKMSQuantilesMT(quantiles);
        estimate(estimator);
    }

    @Test
    public void testQueue() {
        Quantiles estimator = new CKMSQuantilesQueue(quantiles);
        estimate(estimator);
    }

    @Test
    public void testPrimitive() {
        Quantiles estimator = new CKMSQuantilesPrimitive(quantiles);
        estimate(estimator);
    }

    @Test
    public void testThreadLocal() {
        Quantiles estimator = new CKMSQuantilesThreadLocal(quantiles);
        estimate(estimator);
    }

    private void estimate(Quantiles estimator) {
        double[] dataArray = CKMSQuantilesTest.dataArray.clone();
        for (double v : dataArray) {
            estimator.observe(v);
        }
        estimator.flushBuffer();
        Arrays.sort(dataArray);

        System.out.println(estimator.getClass().getSimpleName());
        for (Quantile q : quantiles) {
            double estimate = estimator.get(q.getQuantile());
            double actual = dataArray[(int) (q.getQuantile() * (size - 1))];
            double off = Math.abs(actual - estimate) / size;
            System.out.println(String.format("Q(%.7f, %.7f) is %.7f (actual %.7f, off by %.7f)",
                    q.getQuantile(), q.getError(), estimate, actual,  off));
        }


        System.out.println("# of samples: " + estimator.getSampleSize());
        System.out.println("# of data: " + estimator.getSize());
        System.out.println();
    }

    private static void shuffle(double[] dataArray, Random rnd) {
        for (int i = dataArray.length; i > 1; i--) {
            int j = rnd.nextInt(i);
            double tmp = dataArray[i-1];
            dataArray[i-1] = dataArray[j];
            dataArray[j] = tmp;

        }
    }

}