package com.atguigu.mapreducetest.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author smh
 * @create 2021-04-22 14:08
 */
public class JoinMapper extends Mapper<LongWritable, Text,Text,JoinBean> {

    private String name;
    private FileSplit fileSplit;
    private JoinBean joinBean = new JoinBean();
    private Text outKey = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        fileSplit = (FileSplit) context.getInputSplit();
        name = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        if(name.contains("order")){
            outKey.set(split[1]);
            joinBean.setId(split[0]);
            joinBean.setPid(split[1]);
            joinBean.setAmount(Integer.parseInt(split[2]));
            joinBean.setpName("");
            joinBean.setFlag(name);

        }else{
            outKey.set(split[0]);
            joinBean.setId("");
            joinBean.setPid(split[0]);
            joinBean.setAmount(0);
            joinBean.setpName(split[1]);
            joinBean.setFlag(name);
        }
        context.write(outKey,joinBean);
    }
}
