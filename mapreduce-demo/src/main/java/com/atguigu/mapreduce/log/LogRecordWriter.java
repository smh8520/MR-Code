package com.atguigu.mapreduce.log;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author smh
 * @create 2021-04-20 10:06
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private final FSDataOutputStream atguiguFos;
    private final FSDataOutputStream otherFos;
    private final FileSystem fs;

    public LogRecordWriter(TaskAttemptContext job) throws IOException {
        fs = FileSystem.get(job.getConfiguration());
        atguiguFos = fs.create(new Path("d:/mapreduce/log/atguigu.log1"));
        otherFos = fs.create(new Path("d:/mapreduce/log/other.log1"));
    }


    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String s = key.toString();
        if(s.contains("atguigu")){
            atguiguFos.writeBytes(s+"\n");
        }else{
            otherFos.writeBytes(s+"\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStreams(otherFos,atguiguFos);
    }
}
