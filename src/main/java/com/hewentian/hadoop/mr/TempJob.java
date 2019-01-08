package com.hewentian.hadoop.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

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
 * TempKeyPair{year=2016, temp=34}	2016-10-01 12:21:05 34
 * TempKeyPair{year=2017, temp=41}	2017-10-02 12:21:02 41
 * TempKeyPair{year=2017, temp=37}	2017-10-01 12:21:02 37
 * TempKeyPair{year=2017, temp=32}	2017-01-01 10:31:12 32
 * TempKeyPair{year=2017, temp=27}	2017-10-03 12:21:02 27
 * TempKeyPair{year=2018, temp=46}	2018-07-02 12:21:02 46
 * TempKeyPair{year=2018, temp=45}	2018-01-01 12:21:02 45
 * TempKeyPair{year=2018, temp=23}	2018-12-01 12:21:02 23
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
    static class TempMapper extends Mapper<LongWritable, Text, TempKeyPair, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (StringUtils.isNotBlank(line)) {
                String[] ss = line.split(" ");

                if (ss.length == 3) {
                    String date = ss[0];
                    TempKeyPair tempKeyPair = new TempKeyPair();
                    tempKeyPair.setYear(Integer.parseInt(date.split("-")[0]));
                    tempKeyPair.setTemp(Integer.parseInt(ss[2]));

                    context.write(tempKeyPair, value);
                }
            }
        }
    }

    static class TempReducer extends Reducer<TempKeyPair, Text, TempKeyPair, Text> {
        @Override
        protected void reduce(TempKeyPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("need: input file and output dir.");
            System.out.println("eg: {HADOOP_HOME}/bin/hadoop jar /home/hadoop/tempStat.jar com.hewentian.hadoop.mr.TempJob /temp.txt /output/temp/");
            System.exit(1);
        }

        try {
            Job job = Job.getInstance();
            job.setJobName("temperature stat demo");
            job.setJarByClass(TempJob.class);

            job.setMapperClass(TempMapper.class);
            job.setReducerClass(TempReducer.class);
            job.setOutputKeyClass(TempKeyPair.class);
            job.setOutputValueClass(Text.class);

            job.setNumReduceTasks(3); // 只有3个年份:2016, 2017, 2018
            job.setPartitionerClass(TempYearPartition.class);
            job.setSortComparatorClass(TempSort.class);
            job.setGroupingComparatorClass(TempGroup.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
