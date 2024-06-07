package com.atguigu.gmall.realtime.dws.function;

import com.atguigu.gmall.realtime.common.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<keyword STRING>"))
public class KwSpilt  extends TableFunction<Row> {
    public void eval(String keywords) {
        List<String> stringList = IkUtil.IKsplit(keywords);
        for (String s : stringList) {
            collect(Row.of(s));
        }
    }
}
