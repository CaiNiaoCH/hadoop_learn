package com.chenhao.mp.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-11-06 9:43
 */
public class KVTextReducer extends Reducer<Text, IntWritable, Text,IntWritable> {
    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //出现的次数
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        v.set(sum);

        //写出
        context.write(key,v);
    }
}
