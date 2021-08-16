package com.atguigu.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author smh
 * @create 2021-04-16 16:50
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1.创建Configuration对象,创建Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.指定driver的Class类型
        job.setJarByClass(WordCountDriver.class);

        //3.指定Mapper,Reducer的Class类型
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4.指定Mapper输出的k,v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.指定最终输出的k,v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setPartitionerClass(WordCountPartitioner.class);
//        job.setNumReduceTasks(2);

//        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineTextInputFormat.setMaxInputSplitSize(job,8388608);

        //6.指定输入,输出的路径
        FileInputFormat.setInputPaths(job,new Path("d:/a/"));
        FileOutputFormat.setOutputPath(job,new Path("d:/mapreduce/wordcount1"));

        //7.提交job
        job.waitForCompletion(true);
        /*
        提交源码:
        1.确定当前环境是yarn还是本地
        2.查看输出路径,如果输出路径没填,或者输出路径已存在,则报异常
        3.提供一个路径存放信息,如果当前环境是yarn:jar,8个配置文件,切片信息
                                           本地:8个配置文件,切片信息
        4.启动的MapTask数量等于切片数量
        切片源码:
        1.切片的数量等于启动的MapTask的数量
        2.切片并不是绝对的按照块大小来切的,而是会有一个1.1倍的溢出值,超过这个溢出值才会切片,避免数据倾斜
        3.切片的大小是可以设置的,通过调整maxSize和minSize
        max(minSize,max(blockSize,maxSize));
        maxSize等于long类型的最大值
        minSize等于1
         */
    }
}
