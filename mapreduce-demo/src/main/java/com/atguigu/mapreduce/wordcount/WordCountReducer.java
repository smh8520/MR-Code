package com.atguigu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author smh
 * @create 2021-04-16 16:48
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        //统计每个单词出现的次数
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
