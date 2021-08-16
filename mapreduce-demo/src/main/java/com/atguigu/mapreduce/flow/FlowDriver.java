package com.atguigu.mapreduce.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author smh
 * @create 2021-04-16 19:43
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建Configuration对象,获取Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.设置驱动使用的Class
        job.setJarByClass(FlowDriver.class);

        //3.设置Mapper,Reducer的Class
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4.设置Mapper输出的键值对类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5.设置最终输出的键值对类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        //6.设置输入输出的路径
        FileInputFormat.setInputPaths(job, new Path("d:/phone_data .txt"));
        FileOutputFormat.setOutputPath(job, new Path("d:/mapreduce/flow28"));

        //7.提交任务
        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }
}
