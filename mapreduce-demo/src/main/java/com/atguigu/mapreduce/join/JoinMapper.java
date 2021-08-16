package com.atguigu.mapreduce.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

/**
 * @author smh
 * @create 2021-04-20 13:43
 */
public class JoinMapper extends Mapper<LongWritable, Text,Text,JoinBean> {
    private String fileName;
    private FileSplit fileSplit;
    private JoinBean joinBean = new JoinBean();
    private Text outK = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if(fileName.contains("order")){
            String[] splits = line.split("\t");

            outK.set(splits[1]);

            joinBean.setId(splits[0]);
            joinBean.setPid(splits[1]);
            joinBean.setAmount(Integer.parseInt(splits[2]));
            joinBean.setpName("");
            joinBean.setFlag(fileName);

        }else{
            String[] splits = line.split("\t");

            outK.set(splits[0]);

            joinBean.setId("");
            joinBean.setPid(splits[0]);
            joinBean.setAmount(0);
            joinBean.setpName(splits[1]);
            joinBean.setFlag(fileName);

        }
        context.write(outK,joinBean);
    }
}
