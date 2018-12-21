package com.hewentian.hadoop.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * <p>
 * <b>QqRecommendMapper</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2018-12-21 14:42:41
 * @since JDK 1.8
 */
public class QqRecommendMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        String line = value.toString();
        if (StringUtils.isNotBlank(line)) {
            String[] sa = line.split("\t");

            if (sa.length == 2) {
                outputCollector.collect(new Text(sa[0]), new Text(sa[1]));
                outputCollector.collect(new Text(sa[1]), new Text(sa[0]));
            }
        }
    }
}
