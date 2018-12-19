package com.hewentian.hadoop.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * <p>
 * <b>WordCountMapper</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2018-12-18 23:06:02
 * @since JDK 1.8
 */
public class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * 每次调用map方法会传入split中的一行数据
     *
     * @param key             该行数据在文件中的位置下标
     * @param value           这行数据
     * @param outputCollector
     * @param reporter
     * @throws IOException
     */
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        String line = value.toString();
        if (StringUtils.isNotBlank(line)) {
            StringTokenizer st = new StringTokenizer(line);

            while (st.hasMoreTokens()) {
                String word = st.nextToken();
                outputCollector.collect(new Text(word), new IntWritable(1)); // map 的输出
            }
        }
    }
}
