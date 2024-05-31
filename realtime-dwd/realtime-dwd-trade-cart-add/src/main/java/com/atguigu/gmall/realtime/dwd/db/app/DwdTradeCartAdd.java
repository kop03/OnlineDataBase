package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseAPP;
import com.atguigu.gmall.realtime.common.base.BaseSQLAPP;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013,4,"dwd_trade_cart_add");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //核心处理逻辑
        // 读取topic_db数据
        createTopicDb(groupId,tableEnv);

        // 筛选加购数据
        Table cartAddTable = tableEnv.sqlQuery("select\n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['user_id'] user_id,\n" +
                "\t`data`['sku_id'] sku_id,\n" +
                "\t`data`['cart_price'] cart_price,\n" +
                "\tif(`type`='insert',`data`['sku_num'],cast(cast(`data`['sku_num'] as bigint) - cast(`old`['sku_num'] as bigint) as STRING)) sku_num,\n" +
                "\t`data`['sku_name'] sku_name,\n" +
                "\t`data`['is_checked'] is_checked,\n" +
                "\t`data`['create_time'] create_time,\n" +
                "\t`data`['operate_time'] operate_time,\n" +
                "\t`data`['is_ordered'] is_ordered,\n" +
                "\t`data`['order_time'] order_time,\n" +
                "\t`data`['source_type'] source_type,\n" +
                "\t`data`['source_id'] source_id,\n" +
                "\tts\n" +
                "from topic_db\n" +
                "where `database`= 'gmall'\n" +
                "and `table`= 'cart_info'\n" +
                "and (`type`='insert' or (\n" +
                "\t`type`='update' and `old`['sku_num'] is not null \n" +
                "\tand cast(`data`['sku_num'] as bigint) > cast(`old`['sku_num'] as bigint)))");


        // 创建kafka输出映射
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_CART_ADD + "(\n" +
                "\tid STRING,\n" +
                "\tuser_id STRING,\n" +
                "\tsku_id STRING,\n" +
                "\tcart_price STRING,\n" +
                "\tsku_num STRING,\n" +
                "\tsku_name STRING,\n" +
                "\tis_checked STRING,\n" +
                "\tcreate_time STRING,\n" +
                "\toperate_time STRING,\n" +
                "\tis_ordered STRING,\n" +
                "\torder_time STRING,\n" +
                "\tsource_type STRING,\n" +
                "\tsource_id STRING,\n" +
                "\tts BIGINT" +
                ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_CART_ADD));

        // 写出筛选的数据到对应的kafka主题
        cartAddTable.insertInto(Constant.TOPIC_DWD_TRADE_CART_ADD).execute();
    }
}
