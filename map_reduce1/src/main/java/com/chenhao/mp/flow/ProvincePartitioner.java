package com.chenhao.mp.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author ChenHao
 * @create 2020-11-06 14:11
 */
public class ProvincePartitioner extends Partitioner<Text,FlowBean> {

    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        //key是手机号
        //value是流量信息

        int partition  =4;
        //获取手机号前3位
        String preNum = text.toString().substring(0,3);

        if ("136".equals(preNum)) {
            partition = 0;
        }
        else if("137".equals(preNum)) {
            partition = 1;
        }
        else if("138".equals(preNum)) {
            partition = 2;
        }
        else if("139".equals(preNum)) {
            partition = 2;
        }

        return partition;
    }
}
