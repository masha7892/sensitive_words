package com.realtime.util;

import avro.shaded.com.google.common.cache.Cache;
import avro.shaded.com.google.common.cache.CacheBuilder;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.HbaseUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

//定义一个名为 AsyncHbaseDimBaseDicFunc 的类，继承自 Flink 的 RichAsyncFunction，用于处理异步操作。
//输入输出类型都是 JSONObject，表示从 Kafka 接收 JSON 数据，并返回增强后的 JSON 数据。
public class AsyncHbaseDimBaseDicFunc extends RichAsyncFunction<JSONObject,JSONObject> {
    //transient确保在对象序列化时，hbaseConn 变量不会被保存到序列化数据中，从而避免可能的序列化异常，并减少不必要的序列化开销。
    private transient Connection hbaseConn;
    private transient Table dimTable;
    // 缓存：RowKey -> dic_name
    private transient Cache<String, String> cache;


    //使用 HbaseUtils 类建立与 HBase 的连接。
    //获取 realtime_v2:dim_base_dic 表的引用。
    //创建一个 Guava 本地缓存类似hashmap，设置最大容量为 1000，写入后 10 分钟过期。
    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = new HbaseUtils("cdh01:2181,cdh02:2181,cdh03:2181").getConnection();
        dimTable = hbaseConn.getTable(TableName.valueOf("e_commerce:dim_base_dic"));
        cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();
        // 最后调用 super.open() 完成父类初始化
        super.open(parameters);
    }


    //异步地处理输入的 JSONObject，从 HBase 中查询数据，并将查询结果丰富到输入对象中
    @Override
    public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) throws Exception {
        //读取appraise字段
        String rowKey = input.getJSONObject("after").getString("appraise");
        //使用md5处理key,打散数据,避免数据倾斜
        //找不到hbase数据,因为存入hbase的数据没有设置md5,注释掉这行
//        String rowKey = MD5Hash.getMD5AsHex(appraise.getBytes(StandardCharsets.UTF_8));
        //检查缓存中是否存在该 RowKey 对应的 dic_name
        String cachedDicName = cache.getIfPresent(rowKey);
        //如果缓存中不存在，则使用 CompletableFuture 异步地从 HBase 中查询数据。
        if (cachedDicName != null) {
            enrichAndEmit(input, cachedDicName, resultFuture);
        }
        CompletableFuture.supplyAsync(() -> {
            Get get = new Get(rowKey.getBytes(StandardCharsets.UTF_8));
            try {
                Result result = dimTable.get(get);
                if (result.isEmpty()) {
                    return null;
                }
                return Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("dic_name")));
            } catch (IOException e) {
                throw new RuntimeException("Class: AsyncHbaseDimBaseDicFunc Line 72 HBase query failed ! ! !",e);
            }
        }).thenAccept(dicName -> {
            if (dicName != null) {
                cache.put(rowKey, dicName);
                enrichAndEmit(input, dicName, resultFuture);
            }else {
                enrichAndEmit(input, "N/A", resultFuture);
            }
        });
    }

    //将查询结果 dic_name 添加到原始 JSON 对象中。
    //使用 ResultFuture 发送处理完成的结果。
    private void enrichAndEmit(JSONObject input, String dicName, ResultFuture<JSONObject> resultFuture) {
        JSONObject after = input.getJSONObject("after");
        after.put("dic_name", dicName);
        resultFuture.complete(Collections.singleton(input));
    }

    //如果异步操作超时，则调用此方法。
    @Override
    public void timeout(JSONObject input, ResultFuture<JSONObject> resultFuture) throws Exception {
        super.timeout(input, resultFuture);
    }

    //关闭连接和资源，并调用父类的 close() 方法。
    @Override
    public void close() throws Exception {
        try {
            if (dimTable != null) dimTable.close();
            if (hbaseConn != null) hbaseConn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        super.close();
    }
}
