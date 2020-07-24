package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

//类型拦截器
//实现逻辑：
//获取body，然后判断body中是否含有start即为启动日志，否则为事件日志,然后将start或者是event添加到header中
public class LogTypeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        //第一步获取body
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));

        //第二步获取header
        Map<String, String> headers = event.getHeaders();

        if (log.contains("start")){
            //启动日志
            headers.put("topic","topic_start");

        }else {
            //事件日志
            headers.put("topic","topic_event");
        }


        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        ArrayList<Event> interceptors = new ArrayList<>();

        for (Event event : events) {

            Event intercept1 = intercept(event);
//            集合用add，map类型用put，kv用set
            interceptors.add(intercept1);

        }


        return interceptors;
    }

    @Override
    public void close() {

    }
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}

