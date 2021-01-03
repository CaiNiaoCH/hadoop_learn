package com.chenhao.mp.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;



/**
 * @author ChenHao
 * @create 2020-11-06 15:45
 * 分区内部排序
 */
public class ProvincePartitioner extends Partitioner<FlowBean, Text> {
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        int partition = 4;
        String preNum = text.toString().substring(0,3);

        if("136".equals(preNum)) {
            partition = 0;
        }
        else if ("137".equals(preNum)) {
            partition = 1;
        }
        else if ("139".equals(preNum)) {
            partition = 3;
        }
        return partition;
    }
}
