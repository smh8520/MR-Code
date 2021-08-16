package com.atguigu.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author smh
 * @create 2021-04-23 22:40
 */
public class ZookeeperClient {

    private ZooKeeper zkClient;
    private int sessionTimeOut;
    private String connectString;

    @Before
    public void init() throws IOException {
        connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        sessionTimeOut = 5000;
        zkClient = new ZooKeeper(connectString, sessionTimeOut, System.out::println);
    }

    @Test
    public void showDir() throws KeeperException, InterruptedException {
        showAllDir("/sanguo");
    }
    public void showAllDir(String path) throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists(path, false);
        if (stat == null) {
            System.out.println("节点不存在!");
            return;
        }
        List<String> children = zkClient.getChildren(path, false);
        if (children != null) {
            for (String child : children) {
                showAllDir(path+"/"+child);
            }
        }
        System.out.println(path);

    }

    @Test
    public void setData()throws Exception {
        Stat stat = zkClient.exists("/xiyou/wukong", false);
        if (stat == null) {
            System.out.println("节点不存在!");
            return;
        }
        zkClient.setData("/xiyou/wukong","jinwubang".getBytes(),stat.getVersion());
    }
    @Test
    public void getData() throws Exception {
        getData("/xiyou/wukong");
        Thread.sleep(1000000000000L);
    }
    public void getData(String path)throws Exception {
        Stat stat = zkClient.exists(path, false);
        if (stat == null) {
            System.out.println("节点不存在!");
            return;
        }
        zkClient.getData(path,event -> {
            try {
                System.out.println(event);
                getData(path);
            } catch (Exception e) {
                e.printStackTrace();
            }
        },stat);
    }
    @Test
    public void getDir() throws KeeperException, InterruptedException {
        getChildren("/");
        Thread.sleep(10000000L);
    }

    public void getChildren(String path) throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists(path, false);
        if (stat == null) {
            System.out.println("节点不存在!");
            return;
        }
        zkClient.getChildren(path,event->{System.out.println(event);
            try {
                getChildren(path);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void create() throws KeeperException, InterruptedException {
        zkClient.create("/sanguo/cc",null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void delete() throws KeeperException, InterruptedException {
        deleteALL("/sanguo");
    }

    public void deleteALL(String path) throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists(path, false);
        if (stat == null) {
            System.out.println("节点不存在!");
            return;
        }
        List<String> children = zkClient.getChildren(path, false);
        if (children != null) {
            for (String child : children) {
                deleteALL(path+"/"+child);
            }
        }
        zkClient.delete(path,stat.getVersion());
    }

    @After
    public void close() throws InterruptedException {
        zkClient.close();
    }
}
