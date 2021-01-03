package com.chenhao.mp.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-11-06 15:13
 */
public class FlowCounSortMapper extends Mapper<LongWritable,Text,FlowBean, Text> {
    FlowBean k = new FlowBean();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();

        //2.分割数据
        String[] fields = line.split("\t");

        //3.封装FlowBean对象
        String phoneNum = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        k.set(upFlow,downFlow);

        v.set(phoneNum);

        //4.写出
        context.write(k,v);
    }
}
