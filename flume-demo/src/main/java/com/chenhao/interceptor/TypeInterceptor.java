package com.chenhao.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author ChenHao
 * @create 2020-12-28 19:34
 */
public class TypeInterceptor implements Interceptor {

    private List<Event> addHeaderEvents;

    public void initialize() {
        addHeaderEvents = new ArrayList<Event>();
    }
    /*
    单个事件拦截
     */
    public Event intercept(Event event) {
        //1.获取事件中的头信息
        Map<String, String> headers = event.getHeaders();

        //2.获取事件中的body信息
        String body = new String(event.getBody());

        //3.根据body中的信息给headers取不同的值
        if (body.contains("hello")) {
            headers.put("type","world");
        } else {
            headers.put("type","earth");
        }

        return event;
    }

    /*
    批量事件拦截
     */
    public List<Event> intercept(List<Event> list) {
        //1.清空集合
        addHeaderEvents.clear();

        //2.遍历events
        for (Event event : list) {
            //3.给每一个事件添加头信息
             list.add(intercept(event));
        }
        return addHeaderEvents;
    }

    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        public Interceptor build() {
            return new TypeInterceptor();
        }

        public void configure(Context context) {

        }
    }
}
