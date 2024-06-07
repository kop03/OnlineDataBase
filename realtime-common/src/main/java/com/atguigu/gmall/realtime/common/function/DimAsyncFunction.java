package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HbaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T>{
    StatefulRedisConnection<String, String> redisAsyncConnection;
    AsyncConnection hBaseAsyncConnection;
    @Override
    public void open(Configuration parameters) throws Exception {
        redisAsyncConnection = RedisUtil.getRedisAsyncConnection();
        hBaseAsyncConnection = HbaseUtil.getHBaseAsyncConnection();

    }

    @Override
    public void close() throws Exception {
        RedisUtil.closeRedisAsyncConnection(redisAsyncConnection);
        HbaseUtil.closeAsyncHbaseConnection(hBaseAsyncConnection);
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        String tableName = getTableName();
        String rowKey = getId(input);
        String redisKey = RedisUtil.getRedisKey(tableName, rowKey);
        ForkJoinPool customThreadPool = new ForkJoinPool(10); // 设置并行度为20
        // java的异步编程方式
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                // 第一步异步访问得到的数据
                RedisFuture<String> dimSkuInfoFuture = redisAsyncConnection.async().get(redisKey);
                // 代码里面使用 一定要让dimInfo等于null 直接声明的话会报错
                String dimInfo = null;
                try {
                    dimInfo = dimSkuInfoFuture.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return dimInfo;
            }

        },customThreadPool).thenApplyAsync(new Function<String, JSONObject>() {
            @Override
            public JSONObject apply(String dimInfo) {
                JSONObject dimJsonObj = null;
                // 旁路缓存判断
                if (dimInfo == null || dimInfo.length() == 0){
                    System.err.println("redis不存在数据");
                    try {
                        dimJsonObj = HbaseUtil.getAsyncCells(hBaseAsyncConnection, Constant.HBASE_NAMESPACE, tableName, rowKey);
//                        System.err.println(tableName+"===>"+rowKey);
                        // 将读取的数据保存到redis
                        redisAsyncConnection.async().setex(redisKey,24 * 60 * 60,dimJsonObj.toJSONString());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                else {
                    // redis中存在缓存数据
                    dimJsonObj = JSONObject.parseObject(dimInfo);
                }
                return dimJsonObj;
            }

        }).thenAccept(new Consumer<JSONObject>() {
            @Override
            public void accept(JSONObject dim) {
                // 合并维度信息
                if (dim == null){
                    System.out.println("无法关联当前的维度信息"+ tableName + ":" + rowKey);
                }
                else {
                    join(input,dim);
                }

                // 返回结果
                resultFuture.complete(Arrays.asList(input));
            }
        });
    }


}
