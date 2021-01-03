package com.chenhao.mp.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-11-11 19:18
 */
public class OneIndexMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);
    String name;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件名
        FileSplit split = (FileSplit) context.getInputSplit();

        name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.读取一行
        String line = value.toString();

        //2.切割
        String[] fields = line.split(" ");

        for (String word : fields) {
            //拼接
            k.set(word + "--" + name);
            //写出
            context.write(k,v);
        }

    }
}
