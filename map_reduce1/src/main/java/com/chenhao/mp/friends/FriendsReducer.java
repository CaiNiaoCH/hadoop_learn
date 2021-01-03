package com.chenhao.mp.friends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-11-11 21:38
 */
public class FriendsReducer extends Reducer<Text,Text,Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for (Text person : values) {
            sb.append(person).append(",");
        }
        context.write(key,new Text(sb.toString()));
    }
}
