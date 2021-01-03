package com.chenhao.mp.kv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-11-06 9:46
 */
public class KVTextDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        args = new String[]{"F:/input/kv_input.txt","F:/kv_output"};
        //设置切割夫
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        //1.获取job对象
        Job job = Job.getInstance(conf);
        //设置输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        //2.设置jar包位置，指定使用的mapper和reducer
        job.setJarByClass(KVTextDriver.class);
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);

        //3.设置map输出kv类型
       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(IntWritable.class);

        //4.设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //5.指定输入输出文件路径'
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //6.提交job
        job.waitForCompletion(true);
    }
}
