package com.atguigu.mapreduce.joinn;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;


/**
 * @author smh
 * @create 2021-04-20 15:04
 */
public class JoinDriver {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(JoinDriver.class);

        job.setMapperClass(JoinMapper.class);


        job.setMapOutputKeyClass(JoinBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 5 设置最终输出KV类型
        job.setOutputKeyClass(JoinBean.class);
        job.setOutputValueClass(NullWritable.class);


        job.addCacheFile(new URI("file:///d:/join/pd.txt"));
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job,new Path("d:/join/order.txt"));
        FileOutputFormat.setOutputPath(job,new Path("d:/mapreduce/joinn2"));

        job.waitForCompletion(true);
    }
}
