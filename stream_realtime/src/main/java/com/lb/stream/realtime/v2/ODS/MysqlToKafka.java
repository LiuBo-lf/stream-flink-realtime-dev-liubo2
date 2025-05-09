package com.lb.stream.realtime.v2.ODS;

import com.lb.stream.realtime.v1.utils.FlinkSinkUtil;
import com.lb.stream.realtime.v1.utils.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ Package com.lb.stream.realtime.v2.ODS.MysqlToKafka
 * @ Author  liu.bo
 * @ Date  2025/5/9 10:55
 * @ description: 将MySQL的数据实时同步到Kafka的Flink CDC程序
 * @ version 1.0
 */
public class MysqlToKafka {
    public static void main(String[] args) throws Exception {
        // 获取Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为1，确保数据流处理的顺序性
        env.setParallelism(1);

        // 创建MySQL数据源，使用FlinkSourceUtil工具类中的方法获取MySQL CDC数据源
        MySqlSource<String> realtimeV1 = FlinkSourceUtil.getMySqlSource("realtime_v1", "*");

        // 从MySQL数据源创建Flink数据流CDC数据源处理时间戳
        DataStreamSource<String> mySQLSource = env.fromSource(realtimeV1, WatermarkStrategy.noWatermarks(), "MySQL Source");

        // mySQLSource.print();

        KafkaSink<String> topic_db = FlinkSinkUtil.getKafkaSink("topic_db");

        // 将MySQL数据流同步到Kafka Sink
        mySQLSource.sinkTo(topic_db);

        // 执行Flink任务
        env.execute("Print MySQL Snapshot + Binlog");
    }
}
