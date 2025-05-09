package com.lb.stream.realtime.v1.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @ Package com.lb.stream.realtime.v1.bean.CartAddUuBean
 * @ Author  liu.bo
 * @ Date  2025/5/9 10:17
 * @ description: 加购独立用户
 * @ version 1.0
 */
@Data
@AllArgsConstructor
public class CartAddUuBean {
    String stt;
    String edt;
    String curDate;
    Long cartAddUuCt;
}
