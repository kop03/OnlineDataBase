package com.atguigu.realtime.dim.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseAPP;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HbaseUtil;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import com.atguigu.realtime.dim.function.DimBroadcastFunction;
import com.atguigu.realtime.dim.function.DimHbaseSinkFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

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
//        MysqlSource.print();

        // 3.在hbase中创建维度表  涉及到远程连接都要使用富函数 来保证生命周期
        // flinkcdc 监控的数据类型也是json对象
        SingleOutputStreamOperator<TableProcessDim> createTableStream = createHbaseTable(MysqlSource).setParallelism(1);
//        createTableStream.print();

        // 4.做成广播流
        // 广播状态的key用于判断是否是维度表 value用于补充信息写出到hbase
        MapStateDescriptor<String, TableProcessDim> broadcastState = new MapStateDescriptor<>("broadcast_state", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStateStream = createTableStream.broadcast(broadcastState);

        // 5.连接主流和广播流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = connectionStream(jsonObjectSteam, broadcastState, broadcastStateStream);
//        dimStream.print();


        // 6.筛选出需要写出的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumnStream = filterColumn(dimStream);
//        filterColumnStream.print();



        // 7.写出到hbase
        filterColumnStream.addSink(new DimHbaseSinkFunction());

    }

    public SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumn (SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream) {
        return dimStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> value) throws Exception {
                JSONObject jsonObj = value.f0;
                TableProcessDim dim = value.f1;
                String sinkColumns = dim.getSinkColumns();
                List<String> columns = Arrays.asList(sinkColumns.split(","));
                JSONObject data = jsonObj.getJSONObject("data");
                data.keySet().removeIf(key -> !columns.contains(key));

                System.out.println("sinkTable: "+dim.getSinkTable());
                return value;
            }
        });
    }

    public SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connectionStream(SingleOutputStreamOperator<JSONObject> jsonObjectSteam, MapStateDescriptor<String, TableProcessDim> broadcastState, BroadcastStream<TableProcessDim> broadcastStateStream) {
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectStream = jsonObjectSteam.connect(broadcastStateStream);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = connectStream.process(new DimBroadcastFunction(broadcastState)).setParallelism(1);
        return dimStream;
    }

    public SingleOutputStreamOperator<TableProcessDim> createHbaseTable(SingleOutputStreamOperator<String> mysqlSource) {
        SingleOutputStreamOperator<TableProcessDim> createTableStream = mysqlSource.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {
            public Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 获取连接
                connection = HbaseUtil.getConnection();
            }

            @Override
            public void close() throws Exception {
                // 关闭连接
                HbaseUtil.closeConnection(connection);
            }

            @Override
            public void flatMap(String value, Collector<TableProcessDim> out) throws Exception {
                // 使用读取到的数据到hbase中创建与之对应的表格
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    // 需要判断op的类型 类型不同 有的数据存放在before 有的在after
                    String op = jsonObject.getString("op");
                    TableProcessDim dim;
                    if ("d".equals(op)) {
                        dim = jsonObject.getObject("before", TableProcessDim.class);
                        // 当配置表发送一个d类型的数据时 hbase需要删除一张维度表
                        deleteTable(dim);
                        dim.setOp(op);
                    } else if ("c".equals(op) || "r".equals(op)) {
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        createTable(dim);
                        dim.setOp(op);
                    }

                    // 修改的情况 修改名字为存在的维度表sinktable 先把after的维度表删除 后创建 flinkcdc监控更新数据时 会保留一份更新前的数据 不用考虑更新前的数据存在问题
                    else {
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        deleteTable(dim);
                        createTable(dim);
                        dim.setOp(op);
                    }

                    out.collect(dim);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            private void createTable(TableProcessDim dim) {
                String sinkFamily = dim.getSinkFamily();
                // sink_family 有多个值得时候
                String[] split = sinkFamily.split(",");
                try {
                    HbaseUtil.createTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable(), split);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            private void deleteTable(TableProcessDim dim) {
                try {
                    HbaseUtil.dropTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        return createTableStream;
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
