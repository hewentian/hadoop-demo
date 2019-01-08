package com.hewentian.hadoop.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * <p>
 * <b>TempYearPartition</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-01-03 09:23:03
 * @since JDK 1.8
 */
public class TempYearPartition extends Partitioner<TempKeyPair, Text> {
    @Override
    public int getPartition(TempKeyPair tempKeyPair, Text text, int i) {
        return (tempKeyPair.getYear() * 127) % i;
    }
}
