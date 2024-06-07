import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

public class test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(1).addSink(new RichSinkFunction<Integer>() {
            Jedis jedis;
            @Override
            public void open(Configuration parameters) throws Exception {
                jedis = new Jedis("hadoop103",6379);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void invoke(Integer value, Context context) throws Exception {
                jedis.set("test",value.toString());
            }
        });


        env.execute();
    }
}
