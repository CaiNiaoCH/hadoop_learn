package com.chenhao.mp.topn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * @author ChenHao
 * @create 2020-11-11 20:06
 */
public class TopReducer extends Reducer<FlowBean,Text, Text,FlowBean> {

    Text k = new Text();
    FlowBean v= new FlowBean();
    //定义一个TreeMap作为存储数据的容器（天然按key排序）
    private TreeMap<FlowBean,Text> flowMap = new TreeMap<FlowBean, Text>();

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            FlowBean bean = new FlowBean();
            bean.set(key.getDownFlow(),key.getUpFlow());

            //1 向treemap集合中添加数据
            flowMap.put(bean,new Text(value));

            //2 限制treemap集合中的数据为10条
            if (flowMap.size() > 10) {
                flowMap.remove(flowMap.lastKey());
            }

        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<FlowBean> bean = flowMap.keySet().iterator();
        while(bean.hasNext()) {
            v = bean.next();
            k = flowMap.get(v);
            context.write(k,v);
        }

    }
}
