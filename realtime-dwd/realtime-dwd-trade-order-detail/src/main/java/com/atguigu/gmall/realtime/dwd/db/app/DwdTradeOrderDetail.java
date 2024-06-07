package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLAPP;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderDetail extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10024,4,"dwd_trade_order_detail");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {

        // 在flink sql中使用join一定要添加状态的存活时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));

        // 获取topic_db数据
        createTopicDb(groupId,tableEnv);

        // 筛选订单详情表数据
        Table odTable = tableEnv.sqlQuery("select\n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['order_id'] order_id,\n" +
                "\t`data`['sku_id'] sku_id,\n" +
                "\t`data`['sku_name'] sku_name,\n" +
                "\t`data`['order_price'] order_price,\n" +
                "\t`data`['sku_num'] sku_num,\n" +
                "\t`data`['create_time'] create_time,\n" +
                "\t`data`['split_total_amount'] split_total_amount,\n" +
                "\t`data`['split_activity_amount'] split_activity_amount,\n" +
                "\t`data`['split_coupon_amount'] split_coupon_amount,\n" +
                "\tts\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail'\n" +
                "and `type`='insert' \n");
        tableEnv.createTemporaryView("order_detail",odTable);
//        tableEnv.sqlQuery("select * from order_detail").execute().print();


        // 筛选订单信息表
        Table oiTable = tableEnv.sqlQuery("select\n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['user_id'] user_id,\n" +
                "\t`data`['province_id'] province_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_info'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_info",oiTable);


        // 筛选订单详情活动关联表
        Table odaTable = tableEnv.sqlQuery("select\n" +
                "\t`data`['order_detail_id'] id,\n" +
                "\t`data`['activity_id'] activity_id,\n" +
                "\t`data`['activity_rule_id'] activity_rule_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail_activity'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail_activity",odaTable);


        // 筛选订单详情优惠劵关联表
        Table odcTable = tableEnv.sqlQuery("\n" +
                "select\n" +
                "\t`data`['order_detail_id'] id,\n" +
                "\t`data`['coupon_id'] coupon_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail_coupon'\n" +
                "and `type`='insert' \n" +
                "\t");
        tableEnv.createTemporaryView("order_detail_coupon",odcTable);


        // 将四张表格join合并
        Table joinTable = tableEnv.sqlQuery("select\n" +
                "  od.id,\n" +
                "  order_id,\n" +
                "  sku_id,\n" +
                "  user_id,\n" +
                "  province_id,\n" +
                "  activity_id,\n" +
                "  activity_rule_id,\n" +
                "  coupon_id,\n" +
                "  sku_name,\n" +
                "  order_price,\n" +
                "  create_time,\n" +
                "  sku_num,\n" +
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount\n," +
                "  ts \n" +
                "from order_detail od\n" +
                "join order_info oi\n" +
                "on od.order_id=oi.id\n" +
                "left join order_detail_activity oda \n" +
                "on oda.id=od.id\n" +
                "left join order_detail_coupon odc\n" +
                "on odc.id=od.id ");


        // 写出到kafka中
        // 一旦使用了left join会产生撤回流 此时如果需要写出到kafka 不能使用一般的kafka sink 必须使用upsert kafka
        tableEnv.executeSql("create table " + "dwd_trade_order_detail" + "(\n" +
                "  id STRING,\n" +
                "  order_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  user_id STRING,\n" +
                "  province_id STRING,\n" +
                "  activity_id STRING,\n" +
                "  activity_rule_id STRING,\n" +
                "  coupon_id STRING,\n" +
                "  sku_name STRING,\n" +
                "  order_price STRING,\n" +
                "  create_time STRING,\n" +
                "  sku_num STRING,\n" +
                "  split_total_amount STRING,\n" +
                "  split_activity_amount STRING,\n" +
                "  split_coupon_amount STRING,\n" +
                "  ts bigint,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED \n" +
                ")" + SQLUtil.getUpsertKafkaSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        joinTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute();


    }
}
