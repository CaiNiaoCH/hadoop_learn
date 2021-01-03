package com.chenhao.mp.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * @author ChenHao
 * @create 2020-11-11 19:59
 */
public class TopMapper extends Mapper<LongWritable, Text,FlowBean,Text> {

    //定义一个TreeMap作为存储数据的容器（天然按key排序）
    private TreeMap<FlowBean,Text> flowMap = new TreeMap<FlowBean, Text>();

    private FlowBean kBean;
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();
        kBean = new FlowBean();

        //2.切割
        String[] fields = line.split("\t");

        //3.封装数据
        String phoneNum = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        long sumFlow = Long.parseLong(fields[3]);

        kBean.setDownFlow(downFlow);
        kBean.setUpFlow(upFlow);
        kBean.setSumFlow(sumFlow);

        v.set(phoneNum);

        //向TreeMap中添加数据
        flowMap.put(kBean,v);

        //限制TreeMap的数据量，超过10条就删掉流量最小的一条数据
        if (flowMap.size() > 10) {
            flowMap.remove(flowMap.lastKey());
        }


    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<FlowBean> bean = flowMap.keySet().iterator();
        while(bean.hasNext()) {
            FlowBean k = bean.next();
            context.write(k,flowMap.get(k));
        }
    }
}
