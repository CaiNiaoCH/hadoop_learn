import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author ChenHao
 * @create 2020--17 10:50
 */
public class MyUDF extends UDF{
    public int evaluate(int data) {
        return data + 5;
    }
}
