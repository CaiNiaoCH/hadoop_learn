package com.chenhao.mp.nl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-11-06 9:56
 */
public class NLineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text k = new Text();
    private LongWritable v = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 获取一行
        String line = value.toString();

        // 2 切割
        String[] splited = line.split(" ");

        // 3 循环写出
        for (int i = 0; i < splited.length; i++) {

            k.set(splited[i]);

            context.write(k, v);
        }

    }
}
