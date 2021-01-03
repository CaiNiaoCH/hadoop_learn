package com.chenhao.mp.word;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-11-04 19:02
 */
public class WordcountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"F:/input/wordcount.txt","F:/word_output"};
        //1.获取配置信息及封装任务
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 开启map端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);
        // 设置map端输出压缩方式
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);


        //2.设置jar加载路径
        job.setJarByClass(WordcountDriver.class);

        //3.设置map和reduce类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        //4.设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置Combiner
        //job.setCombinerClass(WordcountReducer.class);
        //job.setCombinerClass(WordCountCombiner.class);

        //job.setNumReduceTasks(2);

        //6.设置输入和输出路劲
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 设置reduce端输出压缩开启
       // FileOutputFormat.setCompressOutput(job, true);

        // 设置压缩的方式
        //FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);


        //7.提交
        job.waitForCompletion(true);
    }

}
