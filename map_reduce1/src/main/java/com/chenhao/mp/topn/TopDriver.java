package com.chenhao.mp.topn;

import com.chenhao.mp.flow.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-11-11 20:25
 */
public class TopDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"F:/input/top10data.txt","F:/top10_output"};
        Configuration conf = new Configuration();
        //1.获取job对象
        Job job = Job.getInstance(conf);
        job.setJarByClass(TopDriver.class);

        // 2 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(TopMapper.class);
        job.setReducerClass(TopReducer.class);

        // 3 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(com.chenhao.mp.topn.FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 4 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 5 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
    }
}
