package com.atguigu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author smh
 * @create 2021-04-16 20:56
 */
public class WordCountPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        char c = text.toString().charAt(0);
        int x;
        if(c <= 'p'){
            x=0;
        }else{
            x=1;
        }
        return x;
    }
}
