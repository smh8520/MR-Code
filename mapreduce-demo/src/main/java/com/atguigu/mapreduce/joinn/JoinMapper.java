package com.atguigu.mapreduce.joinn;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @author smh
 * @create 2021-04-20 13:43
 */
public class JoinMapper extends Mapper<LongWritable, Text,JoinBean, NullWritable> {
   private  HashMap<String, String> hashMap = new HashMap<>();
   private JoinBean joinBean = new JoinBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream open = fileSystem.open(new Path(cacheFiles[0]));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(open,"utf-8"));
        String line;
        while(StringUtils.isNotEmpty(line=bufferedReader.readLine())){
            String[] split = line.split("\t");
            hashMap.put(split[0], split[1]);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("\t");
        joinBean.setId(split[0]);
        joinBean.setpName(hashMap.get(split[1]));
        joinBean.setAmount(Integer.parseInt(split[2]));

        context.write(joinBean,NullWritable.get());
    }
}
