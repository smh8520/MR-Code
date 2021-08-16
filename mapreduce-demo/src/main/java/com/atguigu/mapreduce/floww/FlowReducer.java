package com.atguigu.mapreduce.floww;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author smh
 * @create 2021-04-19 18:13
 */
public class FlowReducer extends Reducer<FlowBean,Text,Text,FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
