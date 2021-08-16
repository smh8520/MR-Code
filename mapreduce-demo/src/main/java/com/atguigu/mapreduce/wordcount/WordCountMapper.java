package com.atguigu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author smh
 * @create 2021-04-16 16:45
 */
public class WordCountMapper extends Mapper<Object, Text,Text, IntWritable> {
    private Text k = new Text();
    private IntWritable v = new IntWritable(1);
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //读取行数据
        String line = value.toString();
        //根据空格切割
        String[] words = line.split(" ");

        //遍历这一行的单词
        for (String word : words) {
            k.set(word);
            context.write(k,v);
        }
    }
}
