package com.chenhao.mp.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-11-06 15:32
 */
public class FlowCountSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"F:/flow_output","F:/flow_output2"};

        //1.获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.指定jar路径和要使用的mapper、reducer
        job.setJarByClass(FlowCountSortDriver.class);
        job.setMapperClass(FlowCounSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);

        //设置分区类
        job.setPartitionerClass(ProvincePartitioner.class);

        //设置ReduceTask的个数
        job.setNumReduceTasks(5);

        //3.指定map输出的kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //4.指定最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //5.指定输入输出文件的路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //6.提交
        job.waitForCompletion(true);
    }
}
