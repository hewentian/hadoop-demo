package com.hewentian.hadoop.mr;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * <p>
 * <b>TempSort</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-01-03 09:09:37
 * @since JDK 1.8
 */
public class TempSort extends WritableComparator {
    public TempSort() {
        super(TempKeyPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TempKeyPair o1 = (TempKeyPair) a;
        TempKeyPair o2 = (TempKeyPair) b;

        int res = Integer.compare(o1.getYear(), o2.getYear());
        if (res != 0) {
            return res;
        }

        return -Integer.compare(o1.getTemp(), o2.getTemp());
    }
}
