package com.atguigu.mapreducetest.join;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * @author smh
 * @create 2021-04-22 14:31
 */
public class JoinOutputFormat extends FileOutputFormat<JoinBean, NullWritable> {
    @Override
    public RecordWriter<JoinBean, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new JoinRecordWriter(job);
    }
}
