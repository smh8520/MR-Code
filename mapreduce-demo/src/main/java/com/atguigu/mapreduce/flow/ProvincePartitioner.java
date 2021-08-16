package com.atguigu.mapreduce.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author smh
 * @create 2021-04-16 20:48
 */
public class ProvincePartitioner extends Partitioner<Text,FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String phone = text.toString().substring(0,3);
        int partition;
        if("136".equals(phone)){
            partition=0;
        }else if("137".equals(phone)){
            partition=1;
        }else if("138".equals(phone)){
            partition=2;
        }else if("139".equals(phone)){
            partition=3;
        }else{
            partition=4;
        }
        return partition;
    }
}
