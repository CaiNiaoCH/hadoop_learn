package com.chenhao.mp.nl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-11-06 9:57
 */
public class NLineReducer extends Reducer<Text, LongWritable, Text,LongWritable> {
    LongWritable v= new LongWritable();
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        //1.汇总
        for (LongWritable value : values) {
            sum += value.get();
        }
        v.set(sum);

        //2.写出
        context.write(key,v);
    }
}
