package com.hewentian.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * <p>
 * <b>WordCountJob</b> 是 统计一个文件中单词个数的Job
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2018-12-19 09:05:18
 * @since JDK 1.8
 */
public class WordCountJob {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("need: input file and output dir.");
            System.out.println("eg: {HADOOP_HOME}/bin/hadoop jar /home/hadoop/wordCount.jar com.hewentian.hadoop.mr.WordCountJob /README.txt /output/wc/");
            System.exit(1);
        }

        Configuration conf = new Configuration();
//      conf.set();

        try {
            Job job = Job.getInstance(conf, "word count mapreduce demo");
            job.setJarByClass(WordCountJob.class);

            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // mapreduce 输入数据所在的目录或文件
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // mr执行之后的输出数据的目录
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
