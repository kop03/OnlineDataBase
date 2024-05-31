package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLAPP;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionCommentInfo extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012,4,"dwd_interaction_comment_info");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env,String groupId) {
        // 核心逻辑
        // 读取topic_db
        createTopicDb(groupId,tableEnv);

        // 读取base_dic
        createBaseDic(tableEnv);

        // 清洗topic_db 筛选出评论信息表新增的数据
        Table commentInfo = tableEnv.sqlQuery("select \n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['user_id'] user_id,\n" +
                "\t`data`['nick_name'] nick_name,\n" +
                "\t`data`['head_img'] head_img,\n" +
                "\t`data`['sku_id'] sku_id, \n" +
                "\t`data`['spu_id'] spu_id, \n" +
                "\t`data`['order_id'] order_id, \n" +
                "\t`data`['appraise'] appraise, \n" +
                "\t`data`['comment_txt'] comment_txt, \n" +
                "\t`data`['create_time'] create_time,\n" +
                "\t`data`['operate_time'] operate_time,\n" +
                "\tproc_time\n" +
                "from topic_db \n" +
                "where `database`='gmall' \n" +
                "and `table`='comment_info' \n" +
                "and `type`='insert'\n" +
                "");

        tableEnv.createTemporaryView("comment_info",commentInfo);
//        Table result = tableEnv.sqlQuery("SELECT * FROM comment_info");


        // 使用lookup join完成维度退化
        Table joinTable = tableEnv.sqlQuery("select\n" +
                "\tid,\n" +
                "\tuser_id,\n" +
                "\tnick_name,\n" +
                "\thead_img,\n" +
                "\tsku_id,\n" +
                "\torder_id,\n" +
                "\tappraise appraise_code,\n" +
                "\tinfo.dic_name appraise_name,\n" +
                "\tcomment_txt,\n" +
                "\tcreate_time,\n" +
                "\toperate_time\n" +
                "from comment_info c\n" +
                "join base_dic FOR SYSTEM_TIME AS OF c.proc_time as b\n" +
                "on c.appraise = b.rowkey");

        // 创建kafka sink对应的表格
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO + "(" +
                        "id STRING,\n" +
                        "user_id STRING,\n" +
                        "nick_name STRING,\n" +
                        "head_img STRING,\n" +
                        "sku_id STRING,\n" +
                        "order_id STRING,\n" +
                        "appraise_code STRING,\n" +
                        "appraise_name STRING,\n" +
                        "comment_txt STRING,\n" +
                        "create_time STRING,\n" +
                        "operate_time STRING"
                + ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO)
                );

        // 写出到对应的kafka主题
        joinTable.insertInto(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO).execute();
    }
}
