package com.lb.stream.realtime.v1.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ Package com.lb.stream.realtime.v1.bean.TableProcessDwd
 * @ Author  liu.bo
 * @ Date  2025/5/9 10:19
 * @ description: 
 * @ version 1.0
*/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDwd {
    // 来源表名
    String sourceTable;

    // 来源类型
    String sourceType;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 配置表操作类型
    String op;
}
