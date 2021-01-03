package com.chenhao.mp.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-11-11 19:42
 */
public class TwoIndexReducer extends Reducer<Text, Text,Text, Text> {
    Text v = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //atguigu a.txt	3

        //1.拼接
        StringBuffer sb = new StringBuffer();
        for (Text value : values) {
            sb.append(value.toString().replace("\t","-->") + "\t");
        }
        v.set(sb.toString());
        context.write(key,v);
    }
}
