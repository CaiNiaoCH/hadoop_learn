package com.chenhao.mp.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020--06 21:13
 */
public class FliterRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream atguiguOut = null;
    FSDataOutputStream otherOut = null;

    public FliterRecordWriter(TaskAttemptContext job) {
        //1.获取文件系统
        FileSystem fs;
        try {
            fs = FileSystem.get(job.getConfiguration());

            //2.创建输出文件路径
            Path atguiguPath = new Path("F:/atguigu.log");
            Path otherPath = new Path("F:/other.log");

            //3.创建输出流
            atguiguOut = fs.create(atguiguPath);
            otherOut = fs.create(otherPath);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        if (key.toString().contains("atguigu")) {
            atguiguOut.write(key.toString().getBytes());
        } else {
            otherOut.write(key.toString().getBytes());
        }
    }

    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}
