package com.chenhao.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author ChenHao
 * @create 2020-11-04 11:28
 */
public class HDFSClient {
    @Test
    public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        // configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
        // FileSystem fs = FileSystem.get(configuration);

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "chenhao");

        // 2 创建目录
        fs.mkdirs(new Path("/user/chenhao/input"));

        // 3 关闭资源
        fs.close();
    }


    //文件上传
    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "chenhao");

        // 2 上传文件
        fs.copyFromLocalFile(new Path("f:/banzhang.txt"), new Path("/banzhang.txt"));

        // 3 关闭资源
        fs.close();

        System.out.println("over");
    }

    //文件下载
    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        //1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "chenhao");

        //2.执行下载操作
        fs.copyToLocalFile(false,new Path("/banzhang.txt"),new Path("f:/banzhang2.txt"),true);

        //3.关闭资源
        fs.close();
    }

    //文件删除
    @Test
    public void testDelete() throws URISyntaxException, IOException, InterruptedException {
        //1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "chenhao");

        //2.执行删除
        fs.delete(new Path("/user"),true);

        //3.关闭资源
        fs.close();
    }

    //文件名更改
    @Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {
        //1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "chenhao");

        //2.执行改名
        fs.rename(new Path("/banzhang.txt"),new Path("/hello.txt"));

        //3.关闭资源
        fs.close();
    }

    //IO流上传
    @Test
    public void putFileToHdfs() throws URISyntaxException, IOException, InterruptedException {
        //1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "chenhao");

        //2.创建输入流
        FileInputStream fis = new FileInputStream(new File("F:/demo.txt"));

        //3.获取输出流
        FSDataOutputStream fos = fs.create(new Path("/demo.txt"));

        //4.流的对拷
        IOUtils.copyBytes(fis,fos,conf);

        //5.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();

    }

    //IO流下载
    @Test
    public void getFileFromHDFS() throws URISyntaxException, IOException, InterruptedException {
        //1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "chenhao");
        //2.获取输入流
        FSDataInputStream fis = fs.open(new Path("/demo.txt"));

        //3.获取输出流
        FileOutputStream fos = new FileOutputStream(new File("F:/demo2.txt"));

        //4.流的对拷
        IOUtils.copyBytes(fis,fos,conf);
        //5.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    //定位文件读取
    //分块读取HDFS上的大文件，比如/user/chenhao/下的/hadoop-2.7.2.tar.gz
    //读取第一块
    @Test
    public void readFileSeek1() throws URISyntaxException, IOException, InterruptedException {
        //1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "chenhao");
        //2.获取输入流
        FSDataInputStream fis = fs.open(new Path("/user/chenhao/hadoop-2.7.2.tar.gz"));

        //3.获取输出流
        FileOutputStream fos = new FileOutputStream(new File("F:/hadoop-2.7.2.tar.gz.part1"));

        //4.流的对拷
        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            fis.read(buf);
            fos.write(buf);
        }

        //5.关闭资源
        fos.close();
        fis.close();
        fs.close();
    }

    //读取第二快
    @Test
    public void readFileSeek2() throws IOException, URISyntaxException, InterruptedException {
        //1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "chenhao");
        //2.获取输入流
        FSDataInputStream fis = fs.open(new Path("/user/chenhao/hadoop-2.7.2.tar.gz"));

        //3.定位输入数据位置
        fis.seek(1024*1024*128);

        //4.获取输出流
        FileOutputStream fos = new FileOutputStream(new File("F:/hadoop-2.7.2.tar.gz.part2"));

        // 5 流的对拷
        IOUtils.copyBytes(fis, fos, conf);

        // 6 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();

    }

}
