package com.chenhao.mp.gp;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-11-06 16:59
 */
public class OrderSortReucer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (NullWritable value : values) {
            if(i < 2) {
                context.write(key,NullWritable.get());
                i++;
            } else{
                break;
            }
        }
    }
}
