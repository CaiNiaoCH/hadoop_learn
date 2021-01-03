package com.chenhao.mp.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-11-06 21:09
 */
public class FliterReducer extends Reducer<Text, NullWritable, Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = key.toString();

        //2.拼接
        line = line + "\r\n";

        //3.写出
        context.write(key,NullWritable.get());
    }
}
