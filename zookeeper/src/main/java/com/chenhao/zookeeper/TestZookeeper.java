package com.chenhao.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author ChenHao
 * @create 2020-12-22 9:51
 */
public class TestZookeeper {

    private String connectString = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                List<String> children = null;
                try {
                    children = zkClient.getChildren("/", true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("-------------------start-------------------");
                for (String child : children) {
                    System.out.println(child);
                }
                System.out.println("-------------------end-------------------");
            }
        });
    }

    //1.创建节点
    @Test
    public void create() throws KeeperException, InterruptedException {
        String path = zkClient.create("/chenhao","orange".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }

    //2.获取子节点并监听数据的变化
    @Test
    public void getChidren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }

        //延时阻塞
        Thread.sleep(Long.MAX_VALUE);
    }

    //3.判断节点是否存在
    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/sanguo", false);

        System.out.println(stat == null ? "not exist" : "exist");
    }


}
