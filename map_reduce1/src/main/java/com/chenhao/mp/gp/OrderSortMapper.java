package com.chenhao.mp.gp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-11-06 16:54
 */
public class OrderSortMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    OrderBean k = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();
        //2.切割
        String[] fields = line.split("\t");

        //3.封装OrderBean对象
        k.setOrder_id(Integer.parseInt(fields[0]));
        k.setPrice(Double.parseDouble(fields[2]));

        //4.写出
        context.write(k,NullWritable.get());
    }
}