package com.chenhao.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-12-22 10:23
 */
public class DistributeServer {

    private String connectString = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;

    public void getConnect() throws IOException {
       zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent event) {

            }
        });
    }

    public void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    public void regist(String hostname) throws KeeperException, InterruptedException {
        String path = zkClient.create("/servers/server",hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(path + "is online");
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        DistributeServer server = new DistributeServer();
        //1.连接zookeeper集群
        server.getConnect();

        //2.注册节点
        server.regist(args[0]);

        //3.业务逻辑处理
        server.business();

    }

}
