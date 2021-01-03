package udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author ChenHao
 * @create 2020-12-17 11:15
 */
public class MyUDF extends UDF {
    public int evaluate(int data) {
        return data+5;
    }

    public int evaluate(int data1,int data2) {
        return data1 + data2 + 5;
    }
}
