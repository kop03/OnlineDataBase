import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class test1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
//                "  `database` STRING,\n" +
//                "  `table` STRING,\n" +
//                "  `ts` bigint,\n" +
//                "  `data` map<STRING,STRING>,\n" +
//                "  `old` map<STRING,STRING>\n" +
//                ") WITH (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'topic_db',\n" +
//                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
//                "  'properties.group.id' = 'test1',\n" +
//                "  'scan.startup.mode' = 'earliest-offset',\n" +
//                "  'format' = 'json'\n" +
//                ");\n");
//
//        Table table = tableEnv.sqlQuery("select \n" +
//                "\t*\n" +
//                "from topic_db \n" +
//                "where `database`='gmall' \n" +
//                "and `table`='comment_info' \n"
//                );

        tableEnv.executeSql("show tables").print();


//        table.execute().print();
    }
}
