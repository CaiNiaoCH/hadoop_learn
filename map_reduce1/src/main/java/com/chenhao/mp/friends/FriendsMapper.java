package com.chenhao.mp.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-11-11 21:27
 * 第一次输出结果,先找出A,B,C、...等是谁的好友
 */
public class FriendsMapper extends Mapper<LongWritable, Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取1行
        String line = value.toString();

        //2.切割
        String[] fields = line.split(":");
        String person = fields[0];
        String[] friends = fields[1].split(",");
        for (String friend : friends) {
            //3.写出<好友，人>
            context.write(new Text(friend),new Text(person));
        }
    }
}
