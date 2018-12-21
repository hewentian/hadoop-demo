package com.hewentian.hadoop.mr;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
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
public class QqRecommendReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        Set<String> set = new HashSet<String>();
        while (values.hasNext()) {
            set.add(values.next().toString());
        }

        if (CollectionUtils.isNotEmpty(set)) {
            for (String name : set) {
                for (String other : set) {
                    if (!StringUtils.equals(name, other)) {
                        outputCollector.collect(new Text(name), new Text(other));
                    }
                }
            }
        }
    }
}
