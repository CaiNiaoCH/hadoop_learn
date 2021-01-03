package com.chenhao.mp.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

/**
 * @author ChenHao
 * @create 2020-11-10 16:53
 */
public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        //压缩
        //compress("F:/input/phone_data.txt","org.apache.hadoop.io.compress.DefaultCodec");
        //compress("F:/input/phone_data.txt","org.apache.hadoop.io.compress.GzipCodec");
        //compress("F:/input/phone_data.txt","org.apache.hadoop.io.compress.BZip2Codec");
        decompress("F:/input/phone_data.txt.gz");
    }

    //压缩
    public static void compress(String fileName,String method) throws IOException, ClassNotFoundException {
        //1 获取输入流
        FileInputStream fis = new FileInputStream(new File(fileName));
        Class<?> codeClass = Class.forName(method);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codeClass, new Configuration());

        //2 获取输出流
        FileOutputStream fos = new FileOutputStream(new File(fileName + codec.getDefaultExtension()));
        //将输出流进行包装
        CompressionOutputStream cos = codec.createOutputStream(fos);

        //3 流的对拷
        IOUtils.copyBytes(fis,cos,1024*1024*5,false);

        //4 关闭资源
        cos.close();
        fos.close();
        fis.close();
    }

    //解压缩
    public static void decompress(String fileName) throws IOException {
        //1.压缩方式检查
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(fileName));

        if (codec == null) {
            System.out.println("can not process");
            return;
        }

        //2.获取输入流
        CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(fileName)));

        //3.获取输出流
        FileOutputStream fos = new FileOutputStream(new File(fileName + ".decoded"));

        //4.流的对拷
        IOUtils.copyBytes(cis,fos,1024*1024*5,false);

        //5.关闭资源
        cis.close();
        fos.close();




    }




}
