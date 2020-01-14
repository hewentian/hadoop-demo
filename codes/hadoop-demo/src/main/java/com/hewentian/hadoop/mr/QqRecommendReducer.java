package com.hewentian.hadoop.mr;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * <p>
 * <b>QqRecommendReducer</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2018-12-21 16:28:40
 * @since JDK 1.8
 */
public class QqRecommendReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> set = new HashSet<String>();
        for (Text t : values) {
            set.add(t.toString());
        }

        if (CollectionUtils.isNotEmpty(set)) {
            for (String name : set) {
                for (String other : set) {
                    if (!StringUtils.equals(name, other)) {
                        context.write(new Text(name), new Text(other));
                    }
                }
            }
        }
    }
}
