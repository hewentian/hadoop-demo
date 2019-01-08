package com.hewentian.hadoop.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
public class QqRecommendMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isNotBlank(line)) {
            String[] sa = line.split("\t");

            if (sa.length == 2) {
                context.write(new Text(sa[0]), new Text(sa[1]));
                context.write(new Text(sa[1]), new Text(sa[0]));
            }
        }
    }
}
