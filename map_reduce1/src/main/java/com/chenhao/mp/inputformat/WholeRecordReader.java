package com.chenhao.mp.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020--06 10:11
 */
public class WholeRecordReader extends RecordReader<Text, BytesWritable> {

    FileSplit split;
    Configuration conf;
    Text k = new Text();
    BytesWritable v = new BytesWritable();
    boolean isProgress = true;
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //初始化
        this.split = (FileSplit) split;
        conf = context.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        //核心业务逻辑处理
        if(isProgress) {

            byte[] buffer = new byte[(int) split.getLength()];

            //1.获取fs对象
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(conf);

            //2.获取输入流
            FSDataInputStream fis = fs.open(path);

            //3.拷贝
            IOUtils.readFully(fis,buffer,0,buffer.length);

            //4.封装v
            v.set(buffer,0,buffer.length);

            //5.封装key
            k.set(path.toString());

            //6.关闭资源
            IOUtils.closeStream(fis);

            isProgress = false;
            return true;

        }
        return  false;
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    public void close() throws IOException {

    }
}
