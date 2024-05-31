package com.atguigu.gmall.realtime.dwd.db.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseAPP;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class DwdBaseDb extends BaseAPP {
    public static void main(String[] args) {
        new DwdBaseDb().start(10019,4, "dwd_base_db",Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 读取topic_db数据
//        stream.print();

        // 清洗过滤和转换
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception exception) {
                    System.out.println("清洗掉脏数据" + value);
                }
            }
        });

        // 读取配置表数据 使用flinkcdc读取
        DataStreamSource<String> tableProcessDwd = env.fromSource(FlinkSourceUtil.getMysqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DWD_TABLE_NAME),
                WatermarkStrategy.noWatermarks(), "table_process_dwd").setParallelism(1);
//        tableProcessDwd.print();

        // 转化数据格式
        SingleOutputStreamOperator<TableProcessDwd> processDwdStream = tableProcessDwd.flatMap(new FlatMapFunction<String, TableProcessDwd>() {
            @Override
            public void flatMap(String value, Collector<TableProcessDwd> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String op = jsonObject.getString("op");
                    TableProcessDwd processDwd;
                    if ("d".equals(op)) {
                        processDwd = jsonObject.getObject("before", TableProcessDwd.class);
                    } else {
                        processDwd = jsonObject.getObject("after", TableProcessDwd.class);
                    }
                    processDwd.setOp(op);
                    out.collect(processDwd);
                } catch (Exception exception) {
                    System.out.println("捕获脏数据" + value);
                }

            }
        }).setParallelism(1);

        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("process_state", String.class,
                TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastStream = processDwdStream.broadcast(mapStateDescriptor);


        // 连接主流和广播流 对主流数据进行判断是否需要保留
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream = jsonObjStream.connect(broadcastStream).process(new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {
            HashMap<String, TableProcessDwd> hashMap = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                Connection mysqlConnection = JdbcUtil.getMysqlConnection();
                List<TableProcessDwd> tableProcessDwds = JdbcUtil.queryList(mysqlConnection, "select * from gmall2023_config.table_process_dwd",
                        TableProcessDwd.class, true);
                for (TableProcessDwd tableProcessDwd : tableProcessDwds) {
                    hashMap.put(tableProcessDwd.getSourceTable() + ":" + tableProcessDwd.getSourceType(), tableProcessDwd);
                }

            }

            @Override
            public void processBroadcastElement(TableProcessDwd value, Context ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                String op = value.getOp();
                String key = value.getSourceTable() + ":" + value.getSourceType();
                if ("d".equals(op)) {
                    broadcastState.remove(key);
                    hashMap.remove(key);
                } else {
                    broadcastState.put(key, value);
                }
            }

            @Override
            public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                // 调用广播状态 判断当前值是否需要保留
                String table = value.getString("table");
                String type = value.getString("type");
                String key = table + ":" + type;
                ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                TableProcessDwd processDwd = broadcastState.get(key);

                // 二次判断 是否是先到的数据
                if (processDwd == null) {
                    processDwd = hashMap.get(key);
                }

                if (processDwd != null) {
                    out.collect(Tuple2.of(value, processDwd));
                }

            }


        }).setParallelism(1);

        processStream.print();

        // 写出到kafka的数据类型都是json字符串 所以先要进行类型转换
        SingleOutputStreamOperator<JSONObject> dataStream = processStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, JSONObject>() {
            @Override
            public JSONObject map(Tuple2<JSONObject, TableProcessDwd> value) throws Exception {
                JSONObject jsonObj = value.f0;
                TableProcessDwd processDwd = value.f1;
                JSONObject data = jsonObj.getJSONObject("data");
                List<String> columns = Arrays.asList(processDwd.getSinkColumns().split(","));
                data.keySet().removeIf(key -> !columns.contains(key));
                data.put("sink_table",processDwd.getSinkTable());
                return data;
            }
        });

        // 将多个表格的数据分别写出到对应的主题
        dataStream.sinkTo(FlinkSinkUtil.getKafkaSinkWithTopicName());
    }
}
