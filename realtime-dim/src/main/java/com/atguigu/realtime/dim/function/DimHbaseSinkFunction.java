package com.atguigu.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HbaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class DimHbaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>>{
    Connection connection;
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HbaseUtil.getConnection();
    }

    @Override
    public void close() throws Exception {
        HbaseUtil.closeConnection(connection);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        JSONObject jsonObj = value.f0;
        TableProcessDim dim = value.f1;

        // maxwell筛选后的type:insert update delete bootstrap-insert
        String type = jsonObj.getString("type");
        JSONObject data = jsonObj.getJSONObject("data");
        if ("delete".equals(type)){
            // 删除对应的维度表数据
            delete(data,dim);
        }
        else {
            System.out.println("data:"+data);
            put(data,dim);
        }
    }

    private void put(JSONObject data, TableProcessDim dim) {

        String sinkTable = dim.getSinkTable();
        String sinkRowKeyName = dim.getSinkRowKey();
        String sinkRowKeyValue= data.getString(sinkRowKeyName);
        String sinkFamily = dim.getSinkFamily();
        HbaseUtil.putCells(connection, Constant.HBASE_NAMESPACE,sinkTable,sinkRowKeyValue,sinkFamily,data);
    }

    private void delete(JSONObject data, TableProcessDim dim) {
        String sinkTable = dim.getSinkTable();
        String sinkRowKeyName = dim.getSinkRowKey();
        String sinkRowKeyValue= data.getString(sinkRowKeyName);
        try {
            HbaseUtil.deleteCells(connection,Constant.HBASE_NAMESPACE,sinkTable,sinkRowKeyValue);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
