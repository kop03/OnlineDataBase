package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class HbaseUtil {
    public static Connection getConnection() {
        // 方法一 使用conf
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM);

        // 方法二 配置文件连接
        Connection connection=null;
        try {
            connection = ConnectionFactory.createConnection(conf);
//            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static void closeConnection(Connection connection){
        try {
            if (connection != null && !connection.isClosed()){
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取到 Hbase 的异步连接
     *
     * @return 得到异步连接对象
     */
    public static AsyncConnection getHBaseAsyncConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭 hbase 异步连接
     *
     * @param asyncConn 异步连接
     */
    public static void closeAsyncHbaseConnection(AsyncConnection asyncConn) {
        if (asyncConn != null) {
            try {
                asyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }



    public static void createTable(Connection connection,String namespace,String table,String... families) throws IOException {
        if (families==null || families.length==0){
            return;
        }
        // 获取admin
        Admin admin = connection.getAdmin();

        // 创建表格描述
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, table));

        for (String family : families) {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
            tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        }

        // 使用admin调用方法创建表格
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            System.out.println("当前表格已经存在  不需要重复创建"+namespace+table);
        }

        // 关闭连接
        admin.close();
    }



    public static JSONObject getCells(Connection connection,String namespace,String tableName,String rowKey) throws IOException {
        // 获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        JSONObject jsonObject = new JSONObject();

        // 调用get方法
        try {
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                jsonObject.put(new String(CellUtil.cloneQualifier(cell)),new String(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 关闭table
        table.close();
        return jsonObject;
    }




    public static void dropTable(Connection connection,String namespace,String table) throws IOException {
        Admin admin = connection.getAdmin();

        try {
            admin.disableTable(TableName.valueOf(namespace,table));
            admin.deleteTable(TableName.valueOf(namespace,table));
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();

    }


    /**
     * 写数据到Hbase
     * @param connection 同步连接
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param family
     * @param data key：value数据 列名和列值
     * @throws IOException
     */

    public static void putCells(Connection connection, String namespace, String tableName, String rowKey, String family, JSONObject data)  {
        // 获取table

        try {
            Table table = null;
            table = connection.getTable(TableName.valueOf(namespace,tableName));
            System.out.println("rowkey"+rowKey);
            // 创建写入对象
            if (rowKey!=null){
                Put put = new Put(rowKey.getBytes());

                for (String column : data.keySet()) {
                    String columnValue = data.getString(column);
                    if (columnValue != null){
                        put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(columnValue));
                    }
                }
                table.put(put);
            }



            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }



    public static void deleteCells(Connection connection, String namespace, String tableName,String rowKey) throws IOException {
        // 获取table
        Table table = connection.getTable(TableName.valueOf(namespace,tableName));

        // 创建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        // 调用方法删除数据
        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }


    public static JSONObject getAsyncCells(AsyncConnection hBaseAsyncConnection,String namespace,String tableName,String rowKey) throws IOException {
        // 获取table
        AsyncTable<AdvancedScanResultConsumer> table = hBaseAsyncConnection.getTable(TableName.valueOf(namespace, tableName));

        // 创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        JSONObject jsonObject = new JSONObject();
        // 第一个get是hbase的api 第二个是java异步访问的api
        try {
            Result result = table.get(get).get();
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                jsonObject.put(Bytes.toString(CellUtil.cloneQualifier(cell)),Bytes.toString(CellUtil.cloneValue(cell)));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return jsonObject;
    }
}
