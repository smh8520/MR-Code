package com.atguigu.mapreduce.etlandcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author smh
 * @create 2021-04-20 18:20
 */
public class EtlMapper extends Mapper<LongWritable,Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter("Etl","Count").increment(1);
        String line = value.toString();
        String[] s = line.split(" ");

        if(s.length>11){
            context.getCounter("Etl","true").increment(1);
            context.write(value,NullWritable.get());
        }else{
            context.getCounter("Etl","false").increment(1);
            return;
        }
    }
}
