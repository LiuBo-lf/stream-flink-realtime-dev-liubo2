package com.lb.stream.realtime.v1.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @ Package com.lb.stream.realtime.v1.bean.TrafficUvCt
 * @ Author  liu.bo
 * @ Date  2025/5/9 10:21
 * @ description: 
 * @ version 1.0
*/
@Data
@AllArgsConstructor
public class TrafficUvCt {
    // 渠道
    String ch;
    // 独立访客数
    Integer uvCt;
}
