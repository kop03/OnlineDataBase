package com.atguigu.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import com.atguigu.realtime.dim.app.DimAPP;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;

public class DimBroadcastFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    public HashMap<String,TableProcessDim> hashMap;
    public MapStateDescriptor<String, TableProcessDim> broadcastState;

    public DimBroadcastFunction(MapStateDescriptor<String, TableProcessDim> broadcastState) {
        this.broadcastState = broadcastState;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 预加载初始的维度表信息
        java.sql.Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(mysqlConnection, "select * from gmall2023_config.table_process_dim",
                TableProcessDim.class, true);
        hashMap=new HashMap<>();
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            tableProcessDim.setOp("r");
            hashMap.put(tableProcessDim.getSinkTable(),tableProcessDim);
        }
        JdbcUtil.closeConnection(mysqlConnection);

    }
    // 问题在这，因为没有初始化维表数据
    @Override
    public void processBroadcastElement(TableProcessDim value, Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        // 读取广播状态
        BroadcastState<String, TableProcessDim> tableProcessState = ctx.getBroadcastState(broadcastState);
        // 将配置表信息作为一个维度表的标记 写到广播状态
        String op = value.getOp();
        if ("d".equals(op)) {
            tableProcessState.remove(value.getSourceTable());
            // 同步删除hashmap中初始化加载的配置表信息
            hashMap.remove(value.getSourceTable());
        } else {
            tableProcessState.put(value.getSourceTable(), value);
        }

    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        // 读取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> tableProcessState = ctx.getBroadcastState(broadcastState);

        // 查询广播状态 判断当前数据对应的表格是否存在于状态里面
        String tableName = "dim_"+value.getString("table");
        TableProcessDim tableProcessDim = tableProcessState.get(tableName);

        // 如果是数据到的太早 造成状态为空 主流的数据永远不会比open方法里的预加载要早
        if (tableProcessDim == null){
            // hashmap里面的值都为维度表
            tableProcessDim  = hashMap.get(tableName);
        }

        if (tableProcessDim != null) {
            // 状态不为空 说明当前一行数据是维度表数据
            out.collect(Tuple2.of(value, tableProcessDim));
        }
    }
}

