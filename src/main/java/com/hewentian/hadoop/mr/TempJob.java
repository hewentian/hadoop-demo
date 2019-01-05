package com.hewentian.hadoop.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * <p>
 * <b>TempJob</b> 是 统计温度的Job
 * 需求：
 * 1. 计算2016-2018年，每年温度最高的时间；
 * 2. 计算2016-2018年，每年温度最高的前10天。
 *
 * 思路：
 * 1. 按年份升序排序，同时每一年的温度降序排序；
 * 2. 按年份分组，每一年对应一个reduce任务;
 * 3. mapper输出的key为一个封装对象；
 * 4. 学习自定义排序、分区、分组。
 * </p>
 * <p>
 * 数据如下：
 * 年-月-日 时：分：秒 温度
 * 2016-10-01 12:21:05 34
 * 2016-10-02 14:01:03 36
 * 2017-01-01 10:31:12 32
 * 2017-10-01 12:21:02 37
 * 2018-12-01 12:21:02 23
 * 2017-10-02 12:21:02 41
 * 2017-10-03 12:21:02 27
 * 2018-01-01 12:21:02 45
 * 2018-07-02 12:21:02 46
 * </p>
 * <p>
 * 执行mapReduce后的输出结果如下：
 * $ ./bin/hdfs dfs -ls /output/temp/
 * Found 4 items
 * -rw-r--r--   3 hadoop supergroup          0 2018-12-31 06:58 /output/temp/_SUCCESS
 * -rw-r--r--   3 hadoop supergroup        110 2018-12-31 06:58 /output/temp/part-00000
 * -rw-r--r--   3 hadoop supergroup        220 2018-12-31 06:58 /output/temp/part-00001
 * -rw-r--r--   3 hadoop supergroup        165 2018-12-31 06:58 /output/temp/part-00002
 * $
 * $ ./bin/hdfs dfs -cat /output/temp/*
 * TempKeyPair{year=2016, temp=36}	2016-10-02 14:01:03 36
 * TempKeyPair{year=2016, temp=36}	2016-10-01 12:21:05 34
 * TempKeyPair{year=2017, temp=41}	2017-10-02 12:21:02 41
 * TempKeyPair{year=2017, temp=41}	2017-10-01 12:21:02 37
 * TempKeyPair{year=2017, temp=41}	2017-01-01 10:31:12 32
 * TempKeyPair{year=2017, temp=41}	2017-10-03 12:21:02 27
 * TempKeyPair{year=2018, temp=46}	2018-07-02 12:21:02 46
 * TempKeyPair{year=2018, temp=46}	2018-01-01 12:21:02 45
 * TempKeyPair{year=2018, temp=46}	2018-12-01 12:21:02 23
 * $
 * $ ./bin/hdfs dfs -cat /output/temp/part-00000
 * TempKeyPair{year=2016, temp=36}	2016-10-02 14:01:03 36
 * TempKeyPair{year=2016, temp=36}	2016-10-01 12:21:05 34
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-01-03 22:22:32
 * @since JDK 1.8
 */
public class TempJob {
    static class TempMapper extends MapReduceBase implements Mapper<LongWritable, Text, TempKeyPair, Text> {
        @Override
        public void map(LongWritable key, Text value, OutputCollector<TempKeyPair, Text> outputCollector, Reporter reporter) throws IOException {
            String line = value.toString();
            if (StringUtils.isNotBlank(line)) {
                String[] ss = line.split(" ");

                if (ss.length == 3) {
                    String date = ss[0];
                    TempKeyPair tempKeyPair = new TempKeyPair();
                    tempKeyPair.setYear(Integer.parseInt(date.split("-")[0]));
                    tempKeyPair.setTemp(Integer.parseInt(ss[2]));

                    outputCollector.collect(tempKeyPair, value);
                }
            }
        }
    }

    static class TempReducer extends MapReduceBase implements Reducer<TempKeyPair, Text, TempKeyPair, Text> {
        @Override
        public void reduce(TempKeyPair key, Iterator<Text> values, OutputCollector<TempKeyPair, Text> outputCollector, Reporter reporter) throws IOException {
            while (values.hasNext()) {
                outputCollector.collect(key, values.next());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("need: input file and output dir.");
            System.out.println("eg: {HADOOP_HOME}/bin/hadoop jar /home/hadoop/tempStat.jar /temp.txt /output/temp/");
            System.exit(1);
        }

        JobConf jobConf = new JobConf(TempJob.class);
        jobConf.setJobName("temperature stat demo");

        jobConf.setMapperClass(TempMapper.class);
        jobConf.setReducerClass(TempReducer.class);
        jobConf.setOutputKeyClass(TempKeyPair.class);
        jobConf.setOutputValueClass(Text.class);

        jobConf.setNumReduceTasks(3); // 只有3个年份:2016, 2017, 2018
        jobConf.setPartitionerClass(TempYearPartition.class);
        jobConf.setOutputKeyComparatorClass(TempSort.class);
        jobConf.setOutputValueGroupingComparator(TempGroup.class);

        FileInputFormat.addInputPath(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        JobClient.runJob(jobConf);
    }
}
