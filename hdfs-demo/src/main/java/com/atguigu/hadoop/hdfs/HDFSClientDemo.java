package com.atguigu.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * @author smh
 * @create 2021-04-14 9:57
 */
public class HDFSClientDemo {
    String user = null;
    Configuration conf = null;
    URI uri = null;
    FileSystem fs = null;

    @Before
    public void createClient() throws IOException, InterruptedException {
        user = "atguigu";
        conf = new Configuration();
        uri = URI.create("hdfs://hadoop102:8020");
        fs = FileSystem.get(uri, conf, user);
    }

    @After
    public void closeClient() throws IOException {
        fs.close();
    }

    @Test
    public void copyFromLocal() throws IOException {
        fs.copyFromLocalFile(false, false, new Path("d:/b.txt"), new Path("/a/a.txt"));
    }

    @Test
    public void copyToLocal() throws IOException {
        fs.copyToLocalFile(true, new Path("/a/a.txt"), new Path("d:/a.txt"), true);
    }

    @Test
    public void delete() throws IOException {
//        fs.delete(new Path("/a/b.txt"),true);
        fs.delete(new Path("/a/b"), true);

    }

    @Test
    public void rename() throws IOException {
        fs.rename(new Path("/a/hdfs.txt"), new Path("/a/a.txt"));
    }

    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> lf = fs.listFiles(new Path("/a/"), true);
        while (lf.hasNext()) {
            System.out.println("=================================");
            LocatedFileStatus next = lf.next();
            System.out.println("路径" + next.getPath());
            System.out.println("长度" + next.getLen());
            System.out.println("组" + next.getGroup());
            System.out.println("主" + next.getOwner());
            System.out.println("权限" + next.getPermission());
            System.out.println("创建时间" + next.getAccessTime());
            String s = "副本数量" + next.getBlockSize();
            System.out.println(s);
        }
     }
}
