package com.atguigu.mapreducetest.joinn;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @author smh
 * @create 2021-04-22 14:55
 */
public class JoinnMapper extends Mapper<LongWritable, Text,JoinnBean, NullWritable> {
    private HashMap<String,String> map = new HashMap<>();
    private JoinnBean joinBean = new JoinnBean();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));
        BufferedReader br = new BufferedReader(new InputStreamReader(fis, "utf-8"));
        String line;

        while (StringUtils.isNotEmpty(line = br.readLine())) {
            String[] split = line.split("\t");
            map.put(split[0], split[1]);
        }
        IOUtils.closeStreams(br,fis,fs);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");

        joinBean.setId(split[0]);
        joinBean.setpName(map.get(split[1]));
        joinBean.setAmount(Integer.parseInt(split[2]));

        context.write(joinBean,NullWritable.get());
    }
}
