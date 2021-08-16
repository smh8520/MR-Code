package com.atguigu.mapreducetest.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author smh
 * @create 2021-04-22 15:34
 */
public class Compress  {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
        CompressionCodec codecByClassName = codecFactory.getCodecByClassName("org.apache.hadoop.io.compress.BZip2Codec");
        FileInputStream fis = new FileInputStream("d:/web.log");
        FileOutputStream fos =new FileOutputStream("d:/web.log"+codecByClassName.getDefaultExtension());
        CompressionOutputStream outputStream = codecByClassName.createOutputStream(fos);
        IOUtils.copyBytes(fis,outputStream,conf);
        IOUtils.closeStreams(outputStream,fos,fis);
    }
}
/*
分区,清洗,combiner,排序,序列化,join,CombinerTextInputFormat,自定义OutputFormat
 */