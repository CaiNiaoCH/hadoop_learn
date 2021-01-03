package com.chenhao.mp.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-11-06 9:16
 */
public class FlowCountReducer extends Reducer<Text,FlowBean, Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long sum_upFlow = 0;
        long sum_downFlow = 0;

        //1.遍历所有bean,将其中的上行流量，下行流量分别累加
        for (FlowBean flowBean : values) {
            sum_upFlow += flowBean.getUpFlow();
            sum_downFlow += flowBean.getDownFlow();

        }

        //2.封装对象
        FlowBean resBean  = new FlowBean(sum_upFlow,sum_downFlow);

        //3.写出
        context.write(key,resBean);
    }
}
