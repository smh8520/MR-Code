package com.atguigu.mapreducetest.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author smh
 * @create 2021-04-22 15:27
 */
public class EtlMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        context.getCounter("ETL","All").increment(1);
        if(split.length>11){
            context.getCounter("ETL","true").increment(1);
            context.write(value,NullWritable.get());
        }else{
            context.getCounter("ETL","false").increment(1);
        }
    }
}
