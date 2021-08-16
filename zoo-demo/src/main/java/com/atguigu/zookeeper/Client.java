package com.atguigu.zookeeper;

import com.sun.xml.internal.bind.v2.runtime.output.StAXExStreamWriterOutput;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author smh
 * @create 2021-04-23 18:31
 */
public class Client {

    private String connectString;
    private ZooKeeper zooKeeper;

    /**
     * 创建客户端对象
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    @Before
    public void init() throws IOException, InterruptedException, KeeperException {
        connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        zooKeeper = new ZooKeeper(connectString, 10000, (event)->{});

    }

    /**
     * 监听子节点信息
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getChildren() throws KeeperException, InterruptedException {
//        List<String> children = zooKeeper.getChildren("/",System.out::println);
//        children.forEach(System.out::println);
        listenerDir("/");
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 递归监听子节点信息
     * @param path
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void listenerDir(String path) throws KeeperException, InterruptedException {
        zooKeeper.getChildren(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
                try {
                    listenerDir(path);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 判断节点是否存在
     */
    @Test
    public void exists() throws KeeperException, InterruptedException {
        Stat exists = zooKeeper.exists("/shuihu", false);
        System.out.println(exists);
    }

    /**
     * 创建子节点
     */
    @Test
    public void create() throws KeeperException, InterruptedException {
        zooKeeper.create("/shuihu/luzhishen","daobayangliu".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE
        ,CreateMode.PERSISTENT);
    }

    /**
     * 监控子节点数据,不监听
     * @throws InterruptedException
     */
    @Test
    public void getData() throws Exception {
        Stat exists = zooKeeper.exists("/shuihu/luzhishen", false);
        if(exists==null){
            System.out.println("节点不存在!");
            return;
        }
        byte[] data = zooKeeper.getData("/shuihu/luzhishen", false, exists);
        System.out.println(new String(data));
    }
    @Test
    public void listenerData() throws Exception {
        listener("/shuihu/luzhishen");
        Thread.sleep(1111111111L);
    }

    /**
     * 递归监视节点数据的变化
     * @param path
     * @throws Exception
     */
    public void listener(String path) throws Exception {
        Stat exists = zooKeeper.exists(path, false);
        if(exists==null){
            System.out.println("节点不存在!");
            return;
        }
        byte[] data = zooKeeper.getData(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
                try {
                    listener(path);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, exists);
        System.out.println(new String(data));
    }

    /**
     * 递归删除
     * @throws Exception
     */
    @Test
    public void delete() throws Exception {
        deleteAll("/xiyou");
    }
    public void deleteAll(String path) throws Exception {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            System.out.println("节点不存在");
            return;
        }
        List<String> children = zooKeeper.getChildren(path, false);
        for (String child : children) {
            deleteAll(path+"/"+child);
        }
        zooKeeper.delete(path,stat.getVersion());
    }

    /**
     * 递归显示所有子节点
     * @throws Exception
     */
    @Test
    public void show() throws Exception {
        showAllDir("/xiyou");
    }
    public void showAllDir(String path) throws Exception {
        Stat stat = zooKeeper.exists(path,false);
        if (stat == null) {
            System.out.println("节点不存在");
            return;
        }
        List<String> children = zooKeeper.getChildren(path, false);

            for (String child : children) {
                showAllDir(path+"/"+child); }

        children.forEach(System.out::println);
    }
    @After
    public void close() throws InterruptedException {

        zooKeeper.close();
    }
}
