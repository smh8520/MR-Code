package com.atguigu.mapreduce.floww;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author smh
 * @create 2021-04-19 18:06
 */
public class FlowMapper extends Mapper<LongWritable, Text,FlowBean,Text> {
    private FlowBean outValue = new FlowBean();
    private Text outKey = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();//读取每一行数据
        String[] splits = line.split("\t");//根据\t对行数据划分

        //读取切片数据给outValue赋值
        outValue.setDownFlow(Long.parseLong(splits[splits.length-2]));
        outValue.setUpFlow(Long.parseLong(splits[splits.length-3]));
        outValue.setSumFlow();
        //读取切片数据给outKey赋值
        outKey.set(splits[1]);
        //写出key,value
        context.write(outValue,outKey);

    }
}
