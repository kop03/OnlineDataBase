package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseAPP;
import com.atguigu.gmall.realtime.common.bean.UserLoginBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.shell.Concat;

import java.time.Duration;

public class DwsUserUserLoginWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsUserUserLoginWindow().start(10024,1,"dws_user_user_login_window", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 读取页面主题数据
//        stream.print();


        // 对数据进行清洗过滤 uid不为空
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);


        // 注册水位线  只需在开窗前注册就行
        SingleOutputStreamOperator<JSONObject> withWaterMarkStream = withWateMark(jsonObjStream);


        // 按照uid分组
        KeyedStream<JSONObject, String> keyedStream = getKeyedStream(withWaterMarkStream);


        // 判断独立用户和回流用户
        SingleOutputStreamOperator<UserLoginBean> uuCtBeanStream = getBackctAndUuctBean(keyedStream);


        // 开窗聚合
        SingleOutputStreamOperator<UserLoginBean> reduceBeanStream = windowAndAgg(uuCtBeanStream);
//        reduceBeanStream.print();


        // 写出到doris
        reduceBeanStream.map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_USER_USER_LOGIN_WINDOW));

    }

    public SingleOutputStreamOperator<UserLoginBean> windowAndAgg(SingleOutputStreamOperator<UserLoginBean> uuCtBeanStream) {
        return uuCtBeanStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(3L)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        return value1;
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (UserLoginBean value : values) {
                            value.setStt(stt);
                            value.setEdt(edt);
                            value.setCurDate(curDt);
                            out.collect(value);
                        }
                    }
                });
    }

    public SingleOutputStreamOperator<UserLoginBean> getBackctAndUuctBean(KeyedStream<JSONObject, String> keyedStream) {
        SingleOutputStreamOperator<UserLoginBean> uuCtBeanStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
            ValueState<String> lastLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastLoginDtDesc = new ValueStateDescriptor<>("last_login_dt", String.class);
                // 不用设置状态存活时间的原因是 当状态失效后无法与现有时间进行判断是否是回流用户
                lastLoginDtState = getRuntimeContext().getState(lastLoginDtDesc);
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<UserLoginBean> out) throws Exception {
                // 比较当前登录的日期和状态存储的日期
                String lastLoginDt = lastLoginDtState.value();
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                // 回流用户数
                Long backCt = 0L;
                // 独立用户数
                Long uuCt = 0L;
                if (lastLoginDt == null) {
                    // 新的访客数据
                    uuCt = 1L;
                    lastLoginDtState.update(curDt);
                } else if (lastLoginDt != null && ts - DateFormatUtil.dateToTs(lastLoginDt) > 7 * 24 * 60 * 60 * 1000L) {
                    // 当前是回流用户 回流用户一定是独立用户
                    backCt = 1L;
                    uuCt = 1L;
                    lastLoginDtState.update(curDt);
                } else if (!lastLoginDt.equals(curDt)) {
                    // 之前有登录 但不是今天
                    uuCt = 1L;
                    lastLoginDtState.update(curDt);
                } else {
                    // 状态不为空 今天的一次登录 状态存储日期等于当前登录日期
                }

                // 不是独立用户肯定不是回流用户 不需要下游统计 （状态存储日期等于当前登录日期）
                if (uuCt != 0) {
                    out.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                }
            }
        });
//        uuCtBeanStream.print();
        return uuCtBeanStream;
    }

    public KeyedStream<JSONObject, String> getKeyedStream(SingleOutputStreamOperator<JSONObject> withWaterMarkStream) {
        return withWaterMarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("uid");
            }
        });
    }

    public SingleOutputStreamOperator<JSONObject> withWateMark(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
        return jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));
    }

    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String uid = jsonObject.getJSONObject("common").getString("uid");
                    String lastPageId = jsonObject.getJSONObject("page").getString("lastPageId");
                    if (uid != null && (lastPageId == null || "login".equals(lastPageId))) {
                        out.collect(jsonObject);
                    }
                } catch (Exception exception) {
                    System.out.println("过滤掉脏数据" + value);
                }

            }
        });
    }
}
