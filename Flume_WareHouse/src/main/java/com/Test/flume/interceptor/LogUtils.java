package com.atguigu.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;

public class LogUtils {
    //验证启动日志
    public static boolean validateStart(String log) {

        if (log == null) {
            return false;
        }

//        判断数据是否是{开头，是否是}结尾
        if (!log.trim().startsWith("{") || !log.trim().endsWith("}")){
            return false;
        }

        return true;
    }
    //验证事件日志
    public static boolean validateEvent(String log) {

        if (log == null) {
            return false;
        }

//        切割
        String[] logContents = log.split("\\|");

        //切割之后数组中有两个元素，判断数组长度是否等于2
        if (logContents.length != 2){
            return false;
        }

        //检验服务器时间戳长度必须13位，而且必须全部是数字
        if (logContents[0].length() != 13 || !NumberUtils.isDigits(logContents[0])){
            return false;
        }

        //检验body中日志格式
        if (!logContents[1].trim().startsWith("{") || !logContents[1].trim().endsWith("}")){
            return false;
        }
        return true;
    }
}
