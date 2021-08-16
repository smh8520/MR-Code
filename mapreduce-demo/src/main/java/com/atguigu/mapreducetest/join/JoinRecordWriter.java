package com.atguigu.mapreducetest.join;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author smh
 * @create 2021-04-22 14:32
 */
public class JoinRecordWriter extends RecordWriter<JoinBean, NullWritable> {
    private final FSDataOutputStream fso;
    private final FileSystem fs;
    private final Configuration conf;

    public JoinRecordWriter(TaskAttemptContext job) throws IOException {
       conf = job.getConfiguration();
        fs = FileSystem.get(conf);
        fso = fs.create(new Path("d:/join/join.txt"));
    }

    @Override
    public void write(JoinBean key, NullWritable value) throws IOException, InterruptedException {
        fso.writeBytes(key.getId()+"\t"+key.getpName()+"\t"+key.getAmount()+"\t"+"哈哈哈哈");
        fso.writeUTF(key.getpName());
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStreams(fso);
    }
}
