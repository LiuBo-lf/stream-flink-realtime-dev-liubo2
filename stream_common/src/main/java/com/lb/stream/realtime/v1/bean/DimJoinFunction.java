package com.lb.stream.realtime.v1.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * @ Package com.lb.stream.realtime.v1.bean.DimJoinFunction
 * @ Author  liu.bo
 * @ Date  2025/5/9 10:18
 * @ description:  
 * @ version 1.0
*/
public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;
}