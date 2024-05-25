package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String groupId,String topicName){
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topicName)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setGroupId(groupId)
                .setValueOnlyDeserializer(
                        // simpleStringScheme无法反序列化null值得数据 会直接报错
                        // 后续dwd层会向kafka中发送null 不能使用SimpleStringSchema
//                        new SimpleStringSchema()
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] message) throws IOException {
                                if (message!=null && message.length!=0){
                                    return new String(message, StandardCharsets.UTF_8);
                                }
                                return "";

                            }

                            @Override
                            public boolean isEndOfStream(String s) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return BasicTypeInfo.STRING_TYPE_INFO;
                            }
                        })
                .build();
    }




    public static MySqlSource<String> getMysqlSource(String databaseName,String tableName){
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .jdbcProperties(props)
                .databaseList(databaseName)
                // 这里必须使用库名.表名的形式
                .tableList(databaseName+"."+tableName)
                .deserializer(new JsonDebeziumDeserializationSchema())   // json数据格式
                // 初始化读取 先把整张表读一遍 后续再去读取变更数据的抓取
                .startupOptions(StartupOptions.initial())
                .build();

        return mysqlSource;
    }
}
