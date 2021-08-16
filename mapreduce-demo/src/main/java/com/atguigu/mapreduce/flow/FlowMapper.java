package com.atguigu.mapreduce.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.webapp.hamlet2.HamletSpec;

import java.io.IOException;

/**
 * @author smh
 * @create 2021-04-16 19:31
 */
public class FlowMapper extends Mapper<Object, Text,Text,FlowBean> {
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");

        outV.setUpFlow(Long.parseLong(split[split.length-2]));
        outV.setDownFlow(Long.parseLong(split[split.length-2]));
        outV.setSumFlow();

        outK.set(split[1]);


        context.write(outK,outV);
    }
}
