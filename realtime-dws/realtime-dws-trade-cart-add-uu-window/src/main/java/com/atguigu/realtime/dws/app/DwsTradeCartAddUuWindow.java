package com.atguigu.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseAPP;
import com.atguigu.gmall.realtime.common.bean.CartAddUuBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.checkerframework.checker.units.qual.K;

import java.time.Duration;

public class DwsTradeCartAddUuWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsTradeCartAddUuWindow().start(10026, 4, "dws_trade_cart_add_uu_window", Constant.TOPIC_DWD_TRADE_CART_ADD);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
//        stream.print();

        // 清洗数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String user_id = jsonObject.getString("user_id");
                    Long ts = jsonObject.getLong("ts");
                    if (user_id != null && ts != null) {
                        // 将时间戳修改为13位毫秒级
                        jsonObject.put("ts",ts * 1000L);
                        out.collect(jsonObject);
                    }
                } catch (Exception exception) {
                    System.out.println("过滤掉脏数据" + value);
                }
            }
        });

        // 添加水位线
        SingleOutputStreamOperator<JSONObject> withWaterMarkStream = jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));


        // 按照user_id进行分组
        KeyedStream<JSONObject, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("user_id");
            }
        });

        
        // 判断是否为独立用户
        SingleOutputStreamOperator<CartAddUuBean> uuctBeanStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {
            ValueState<String> lastLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastLoginDtDesc = new ValueStateDescriptor<>("last_login_dt", String.class);
                lastLoginDtDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                lastLoginDtState = getRuntimeContext().getState(lastLoginDtDesc);

            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<CartAddUuBean> out) throws Exception {
                // 判断独立用户 比较当前数据的时间和状态中的上次登录的时间
                String curDt = DateFormatUtil.tsToDateTime(value.getLong("ts"));
                String lastLoginDt = lastLoginDtState.value();
                if (lastLoginDt == null || !lastLoginDt.equals(curDt)) {
                    // 当前为独立用户
                    lastLoginDtState.update(curDt);
                    out.collect(new CartAddUuBean("", "", "", 1L));
                }

            }
        });


        // 开窗聚合
        SingleOutputStreamOperator<CartAddUuBean> reduceStream = uuctBeanStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                        value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                        return value1;
                    }
                }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {
                        for (CartAddUuBean value : values) {
                            String stt = DateFormatUtil.tsToDateTime(window.getStart());
                            String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                            String curDate = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                            value.setStt(stt);
                            value.setEdt(edt);
                            value.setCurDate(curDate);
                            out.collect(value);
                        }
                    }
                });

//        reduceStream.print();


        // 写出到doris
        reduceStream.map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_CART_ADD_UU_WINDOW));

    }

}
