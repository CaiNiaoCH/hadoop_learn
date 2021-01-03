package com.chenhao.mp.gp;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author ChenHao
 * @create 2020-11-06 16:45
 */
public class OrderBean implements WritableComparable<OrderBean> {
    private int order_id;   //订单id号
    private double price;   //价格

    public OrderBean() {
        super();
    }

    public OrderBean(int order_id, double price) {
        this.order_id = order_id;
        this.price = price;
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
    /*
    * 二次排序
    * Map阶段读取到的所有数据按照id升序，如果id相同再按照金额降序排序，发送到Reduce
    * */
    public int compareTo(OrderBean o) {
        int result;
        if (order_id > o.getOrder_id()) {
            result = 1;
        } else if (order_id < o.getOrder_id()) {
            result = -1;
        } else {        //id相同，按照price降序排序
            return price > o.getPrice() ? -1 : 1;
        }
        return result;
     }

    @Override
    public String toString() {
        return order_id +
                "\t" + price;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(order_id);
        out.writeDouble(price);
    }

    public void readFields(DataInput in) throws IOException {
        order_id = in.readInt();
        price = in.readDouble();
    }
}
