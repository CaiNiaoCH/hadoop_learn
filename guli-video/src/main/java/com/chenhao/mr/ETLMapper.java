package com.chenhao.mr;

import com.chenhao.til.ETLutil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-12-17 20:41
 */
public class ETLMapper extends Mapper<LongWritable, Text, NullWritable,Text> {

    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.获取数据
        String oriStr = value.toString();

        //2.过滤数据
        String etlStr = ETLutil.etlStr(oriStr);

        if(etlStr == null) {
            return;
        }

        //3.写出
        v.set(etlStr);
        context.write(NullWritable.get(),v);


    }
}
