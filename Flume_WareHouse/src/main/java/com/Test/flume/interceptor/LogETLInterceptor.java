package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LogETLInterceptor implements Interceptor {
    //初始化
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //ETL 数据清洗

        //获取到body日志
        byte[] body = event.getBody();

        //转换成字符串
        String log = new String(body, Charset.forName("UTF-8"));

        if (log.contains("start")){
            //验证启动日志的逻辑
            if (LogUtils.validateStart(log)) {
                return event;
            }

        }else {
//            验证事件日志的逻辑
            if (LogUtils.validateEvent(log)) {
                return event;
            }

        }

        return null;
    }

//    多event处理逻辑，循环边event,然后调用单event处理，最后判断成功，返回list集合
    @Override
    public List<Event> intercept(List<Event> events) {

        ArrayList<Event> interceptors = new ArrayList<>();

        //多Event处理循环
        for (Event event : events) {

            //intercept(event)返回正常值，或者是null如上；
            Event intercept1 = intercept(event);

            if (intercept1 != null){
                //能通过的添加进去，否则过滤掉
                interceptors.add(intercept1);
            }
        }
        //校验合格，直接返回
        return interceptors;
    }

    //关闭资源
    @Override
    public void close() {

    }

//    静态内部类，为了实例化对象
    public static class Builder implements Interceptor.Builder{

//        alt+enter
        @Override
        public Interceptor build() {
            //调用自己创建的方法
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
