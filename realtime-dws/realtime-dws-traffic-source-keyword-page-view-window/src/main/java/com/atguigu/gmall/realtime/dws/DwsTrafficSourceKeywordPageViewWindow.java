package com.atguigu.gmall.realtime.dws;

import com.atguigu.gmall.realtime.common.base.BaseSQLAPP;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import com.atguigu.gmall.realtime.dws.function.KwSpilt;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10021,4, "dws_traffic_source_keyword_page_view_window");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        // 核心业务逻辑
        // 读取主流DWD页面主题数据
        tableEnv.executeSql("create table page_info(\n" +
                "\t`common` map<STRING,STRING>,\n" +
                "\t`page` map<STRING,STRING>,\n" +
                "\t`ts` BIGINT,\n" +
                "\t`row_time` as TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "\tWATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                "  )" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRAFFIC_PAGE,groupId));

//        tableEnv.executeSql("select page['last_page_id'],page['item_type'],page['item'] from page_info").print();

        // 筛选关键字keywords
        Table keywordsTable = tableEnv.sqlQuery("select \n" +
                "\t page['item'] keywords,\n" +
                "\t `row_time`\n" +
                "from page_info\n" +
                "where page['last_page_id']='search'\n" +
                "and page['item_type']='keyword'\n" +
                "and page['item'] is not null;");
        tableEnv.createTemporaryView("keywords_table",keywordsTable);


        // 自定义UDTF函数 并注册
        tableEnv.createTemporarySystemFunction("KwSplit", KwSpilt.class);


        // 调用分词函数对keywords进行拆分
        Table keywordTable = tableEnv.sqlQuery(
                "SELECT keywords, keyword,row_time " +
                        "FROM keywords_table " +
                        "LEFT JOIN LATERAL TABLE(Kwsplit(keywords)) ON TRUE");
        tableEnv.createTemporaryView("keyword_table",keywordsTable);


        // 对keyword进行分组开窗聚合
        Table windoaggTable = tableEnv.sqlQuery("SELECT\n" +
                "  keyword,\n" +
                "  cast(TUMBLE_START(row_time, INTERVAL '10' SECOND) as STRING) stt,\n" +
                "  cast(TUMBLE_end(row_time, INTERVAL '10' SECOND) as STRING) ett,\n" +
                "  cast(CURRENT_DATE as STRING) cur_date,\n" +
                "  count(*) keyword_count\n" +
                "FROM keyword_table\n" +
                "GROUP BY\n" +
                "  TUMBLE(row_time, INTERVAL '10' SECOND),\n" +
                "  keyword");


        // 写出到doris
        // flink需要打开检查点才能把数据写出到doris
        tableEnv.executeSql("create table doris_sink(\n" +
                "\tstt STRING,\n" +
                "\tedt STRING,\n" +
                "\tcur_date STRING,\n" +
                "\tkeyword STRING,\n" +
                "\tkeyword_count BIGINT\n" +
                ")" + SQLUtil.getDorisSinkSQL(Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW));

        windoaggTable.insertInto("doris_sink");
    }
}
