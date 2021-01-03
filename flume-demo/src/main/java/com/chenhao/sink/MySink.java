package com.chenhao.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author ChenHao
 * @create 2020-12-29 21:43
 */
public class MySink extends AbstractSink implements Configurable {

    //创建Logger对象
    private static final Logger log = LoggerFactory.getLogger(AbstractSink.class);
    private String prefix;
    private String suffix;

    public void configure(Context context) {
        prefix = context.getString("prefix","eat:");
        suffix = context.getString("suffix","sleep:");
    }

    public Status process() throws EventDeliveryException {
        //1.声明返回值状态信息
        Status status;

        //2.获取当前sink绑定的Channel
        Channel channel = getChannel();

        //3.获取事务
        Transaction transaction = channel.getTransaction();

        //4.声明事件
        Event event;

        //5.开启事务
        transaction.begin();

        //6.读取Channel中的事务，直到读取到事件结束循环
        while (true) {
            event = channel.take();
            if (event != null) {
                break;
            }
        }
        try {
            //处理事务
            log.info(prefix + new String(event.getBody()) + suffix);
            //提交事务
            transaction.commit();
            status = Status.READY;
        } catch (Exception e) {
            //遇到异常，事务回滚
            transaction.rollback();
            status = Status.BACKOFF;
            e.printStackTrace();
        } finally {
            //关闭事务
            transaction.close();
        }
        return status;
    }
}
