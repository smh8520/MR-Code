package com.atguigu.mapreduce.floww;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author smh
 * @create 2021-04-19 18:32
 */
public class FlowPartitioner extends Partitioner<FlowBean,Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        int partition=0;
        String parPhone = text.toString().substring(0, 3);

        if("135".equals(parPhone)){
            partition=0;
        }else if("136".equals(parPhone)){
            partition=1;
        }else if("137".equals(parPhone)){
            partition=2;
        }else if("138".equals(parPhone)){
            partition=3;
        }else{
            partition=4;
        }

        return partition;
    }
}
