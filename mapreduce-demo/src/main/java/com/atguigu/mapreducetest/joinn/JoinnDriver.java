package com.atguigu.mapreducetest.joinn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * @author smh
 * @create 2021-04-22 15:12
 */
public class JoinnDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(JoinnDriver.class);

        job.setMapperClass(JoinnMapper.class);

        job.addCacheFile(new URI("file:///d:/join/pd.txt"));
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(JoinnMapper.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("d:/join/order.txt"));
        FileOutputFormat.setOutputPath(job,new Path("d:/mapreduce/join7"));

        boolean b = job.waitForCompletion(true);

    }
}
