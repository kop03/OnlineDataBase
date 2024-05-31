package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;

public class SQLUtil {
    public static String getKafkaSourceSQL(String topicName,String groupId){
       return "WITH (\n" +
               "  'connector' = 'kafka',\n" +
               "  'topic' = '" + topicName + "',\n" +
               "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
               "  'properties.group.id' = '" + groupId + "',\n" +    // 消费者组id
               "  'scan.startup.mode' = 'earliest-offset',\n" +
               "  'format' = 'json'\n" +
               ")";
    }

    public static String getKafkaTopicDb(String groupId){
        return "CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `data` map<STRING,STRING>,\n" +
                "  `old` map<STRING,STRING>,\n" +
                "  `ts` bigint,\n" +
                "   proc_time as PROCTIME(),\n" +
                "   row_time as TO_TIMESTAMP_LTZ(ts,3),\n" +
                "   WATERMARK FOR row_time AS row_time - INTERVAL '15' SECOND \n" +  // 时间类型必须是timestamp3
                "\n" +
                ")" + getKafkaSourceSQL(Constant.TOPIC_DB,groupId);
    }

    public static String getKafkaSinkSQL(String topicName){
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'format' = 'json'\n" +
                ")";
    }


    // 获取upsert kafka的连接  创建表格的语句最后一定要声明主键
    public static String getUpsertKafkaSQL(String topicName){
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }
}
