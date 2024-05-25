package com.atguigu.realtime.dim.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPObject;
import com.atguigu.gmall.realtime.common.base.BaseAPP;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.google.gson.JsonObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DimAPP extends BaseAPP {
    public static void main(String[] args) {
        new DimAPP().start(10001,4,"dim_app", Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心逻辑
        // 1.对ods读取的原始数据进行数据清洗  json格式的数据输出一般都是json对象
        SingleOutputStreamOperator<JSONObject> jsonObjectSteam = etl(stream);

        // 2.使用flinkcdc读取监控配置表数据
        MySqlSource<String> mysqlSource = FlinkSourceUtil.getMysqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DIM_TABLE_NAME);

        // 3.读取数据
        DataStreamSource<String> MysqlSource = env.fromSource(
                mysqlSource,
                WatermarkStrategy.noWatermarks(),
                "mysql_source"
        ).setParallelism(1);
        MysqlSource.print();

        // 3.在hbase中创建维度表

        // 4.做成广播流

        // 5.连接主流和广播流

        // 6.筛选出需要写出的字段

        // 7.写出到hbase
    }

    public  SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> jsonObjectStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String database = jsonObject.getString("database");
                    String type = jsonObject.getString("type");
                    JSONObject data = jsonObject.getJSONObject("data");
                    if ("gmall".equals(database) && !"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type) && data != null && data.size() != 0) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        return jsonObjectStream;
    }
}
