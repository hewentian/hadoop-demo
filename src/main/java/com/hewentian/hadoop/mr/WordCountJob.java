package com.hewentian.hadoop.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


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
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("need: input file and output dir.");
            System.out.println("eg: {HADOOP_HOME}/bin/hadoop jar /home/hadoop/wordCount.jar /README.txt /output/wc/");
            System.exit(1);
        }

        JobConf jobConf = new JobConf(WordCountJob.class);
        jobConf.setJobName("word count mapreduce demo");

        jobConf.setMapperClass(WordCountMapper.class);
        jobConf.setReducerClass(WordCountReducer.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        // mapreduce 输入数据所在的目录或文件
        FileInputFormat.addInputPath(jobConf, new Path(args[0]));
        // mr执行之后的输出数据的目录
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        JobClient.runJob(jobConf);
    }
}
