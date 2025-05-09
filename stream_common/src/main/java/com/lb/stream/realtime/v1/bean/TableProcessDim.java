package com.lb.stream.realtime.v1.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ Package com.lb.stream.realtime.v1.bean.TableProcessDim
 * @ Author  liu.bo
 * @ Date  2025/5/9 10:19
 * @ description:DIM层封装的类
 * @ version 1.0
*/
@AllArgsConstructor
@NoArgsConstructor
@Data
public class TableProcessDim {
    String sourceTable;
    String sinkTable;
    String sinkColumns;
    String sinkFamily;
    String sinkRowKey;
    String op;
}
