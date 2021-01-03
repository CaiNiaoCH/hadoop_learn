package com.chenhao.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ChenHao
 * @create 2020-12-22 10:45
 */
public class DistributeClient {

    private String connectString = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;

    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/servers", true);

        //存储服务器节点主机名称集合
        ArrayList<String> hosts = new ArrayList<String>();

        for (String child : children) {
            byte[] data = zkClient.getData("/servers/" + child, false, null);
            hosts.add(new String(data));
        }

        //将所有在线主机名称打印到控制台
        System.out.println(hosts);
    }

    public void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent event) {
                try {
                    getChildren();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        DistributeClient client = new DistributeClient();

        //1.获取zookeeper集群链接
        client.getConnect();

        //2.注册监听
        client.getChildren();

        //3.业务逻辑处理
        client.business();

    }
}
