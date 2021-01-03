package com.chenhao.mp.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-11-06 9:39
 */

//统计输入文件中每一行的第一个单词相同的行数
public class KVTextMapper extends Mapper<Text,Text, Text, IntWritable> {
    IntWritable v = new IntWritable(1);
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //mao阶段的key和value是每行第一个单词，其value是1    banzhang  1
        //写出
        context.write(key,v);
    }
}
