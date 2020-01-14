package com.hewentian.hadoop.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * <p>
 * <b>QqRecommendJob</b> 是 qq好友推荐Job，如果A认识B，而B又认识C，那么将A推荐给C
 * map: 输出两次：
 * key:A value:B
 * key:B value:A
 * reduce:
 * 将同一个key下的value产生迪卡尔积
 * </p>
 *
 * <p>
 * hdfs中的文件内容如下：
 * tim	hui
 * jack	hang
 * scott	tiger
 * tiger	cat
 * hui	hang
 * </p>
 *
 * <p>
 * 执行mapReduce后将产生：
 * hui	jack
 * jack	hui
 * hang	tim
 * tim	hang
 * cat	scott
 * scott	cat
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2018-12-21 16:46:50
 * @since JDK 1.8
 */
public class QqRecommendJob {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("need: input file and output dir.");
            System.out.println("eg: {HADOOP_HOME}/bin/hadoop jar /home/hadoop/qqRecommend.jar com.hewentian.hadoop.mr.QqRecommendJob /qq.txt /output/qq/");
            System.exit(1);
        }

        try {
            Job job = Job.getInstance();
            job.setJobName("qq recommend demo");
            job.setJarByClass(QqRecommendJob.class);

            job.setMapperClass(QqRecommendMapper.class);
            job.setReducerClass(QqRecommendReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
