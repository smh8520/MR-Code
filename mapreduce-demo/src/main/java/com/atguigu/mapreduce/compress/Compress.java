package com.atguigu.mapreduce.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

/**
 * @author smh
 * @create 2021-04-21 13:48
 */
public class Compress {
    public static void main(String[] args) throws Exception {
//        compress("d:/join/JaneEyre.txt","org.apache.hadoop.io.compress.DefaultCodec");
//        compress("d:/join/JaneEyre.txt","org.apache.hadoop.io.compress.BZip2Codec");
    }

    private static void compress(String input, String method) throws Exception {
        Configuration conf = new Configuration();
        FileInputStream fis = new FileInputStream(input);

        CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
        CompressionCodec codecByClassName = codecFactory.getCodecByClassName(method);

        FileOutputStream fos = new FileOutputStream(input+codecByClassName.getDefaultExtension());

        CompressionOutputStream outputStream = codecByClassName.createOutputStream(fos);

        IOUtils.copyBytes(fis,outputStream,conf);

        IOUtils.closeStreams(outputStream,fos,fis);
    }
}
