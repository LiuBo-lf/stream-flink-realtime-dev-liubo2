package com.lb.stream.realtime.v1.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @ Package com.lb.stream.realtime.v1.bean.TradeProvinceOrderAmount
 * @ Author  liu.bo
 * @ Date  2025/5/9 10:19
 * @ description: 
 * @ version 1.0
*/
@Data
@AllArgsConstructor
public class TradeProvinceOrderAmount {
    // 省份名称
    String provinceName;
    // 下单金额
    Double orderAmount;
}
