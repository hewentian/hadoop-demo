package com.hewentian.hadoop.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

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
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("need: input file and output dir.");
            System.out.println("eg: {HADOOP_HOME}/bin/hadoop jar /home/hadoop/qqRecommend.jar /qq.txt /output/qq/");
            System.exit(1);
        }

        JobConf jobConf = new JobConf(QqRecommendJob.class);
        jobConf.setJobName("qq recommend demo");

        jobConf.setMapperClass(QqRecommendMapper.class);
        jobConf.setReducerClass(QqRecommendReducer.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        JobClient.runJob(jobConf);
    }
}
