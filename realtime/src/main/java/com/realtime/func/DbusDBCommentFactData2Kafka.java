package com.realtime.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.realtime.util.AsyncHbaseDimBaseDicFunc;
import com.realtime.util.KafkaOffsetUtils;
import com.realtime.util.SensitiveWordsUtils;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import sun.reflect.misc.ConstructorUtil;

import javax.security.auth.login.Configuration;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

//keypoint 评论数据的敏感词过滤
public class DbusDBCommentFactData2Kafka {
    //读取文件数据,按行存入集合
    private static final ArrayList<String> sensitiveWordsLists;
    static{
        sensitiveWordsLists = SensitiveWordsUtils.getSensitiveWordsLists();
    }

    //获取配置
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_cdc_db_topic = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String kafka_db_fact_comment_topic = ConfigUtils.getString("kafka.db.fact.comment.topic");

    @SneakyThrows
    public static void main(String[] args) {
        //指定hadoop执行用户
        System.setProperty("HADOOP_USER_NAME","hdfs");
        //设置流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);



        //全量读取kafka的ods_initial
        DataStreamSource<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        kafka_botstrap_servers,
                        kafka_cdc_db_topic,
                        "DbusDBCommentFactData2Kafka",
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        //接口契约：SerializableTimestampAssigner<T> 的唯一抽象方法就是 extractTimestamp(T, long)，参数不能省略，否则编译报错。
                        .withTimestampAssigner((event, timestamp) -> {
                            if (event != null) {
                                try {
                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                } catch (Exception e) {
                                    //打印异常简要信息,完整调用栈
                                    e.printStackTrace();
                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                    //当 JSONObject.parseObject(event) 抛出任何异常（例如格式错误、字段缺失等）时，
                                    // 捕获异常后返回一个默认时间戳 0L，避免异常继续向上抛出导致程序中断
                                    //0L 对应的时间戳是 1970-01-01 00:00:00 UTC，几乎所有业务时间都会晚于此刻。
                                    // 将无法解析的数据标记为最早时间，可确保不会误放在正常窗口中处理。
                                    return 0L;
                                }
                            }
                            //当上游传下来的 event 为 null 时，不会进入 if 体内的解析逻辑，
                            // 直接执行最后一行的 return 0L，同样返回默认时间戳，保证方法有返回值
                            return 0L;
                        }), "kafka_cdc_db_source"
        );

        kafkaCdcDbSource.print();
//        //过滤出订单主表
//        SingleOutputStreamOperator<JSONObject> filteredOrderInfoStream = kafkaCdcDbSource
//                .map(JSON::parseObject)
//                .filter(jsonObj -> jsonObj.getJSONObject("source").getString("table").equals("order_info"));
//
//        //过滤出订单表,并且使用appraise分组
//        //经过keyBy的流不直接支持 print()
//        KeyedStream<JSONObject, String> filteredCommentInfoStream = kafkaCdcDbSource
//                .map(JSON::parseObject)
//                .filter(jsonObj -> jsonObj.getJSONObject("source").getString("table").equals("comment_info"))
//                .keyBy(jsonObj -> jsonObj.getJSONObject("after").getString("appraise"));
//
//        //异步io到appraise对应的dic_name
//        SingleOutputStreamOperator<JSONObject> enrichedStream = AsyncDataStream
//                .unorderedWait(
//                        filteredCommentInfoStream,
//                        new AsyncHbaseDimBaseDicFunc(),
//                        60,
//                        TimeUnit.SECONDS,
//                        1000
//                );
//
//        //map操作生成新的jsonObj,用于存储提取和整理后的字段
//        SingleOutputStreamOperator<JSONObject> orderCommentMap = enrichedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
//            @Override
//            public JSONObject map(JSONObject jsonObject) throws Exception {
//                //获取ts数据
//                JSONObject jsonObj = new JSONObject();
//                Long tsMs = jsonObj.getLong("ts_ms");
//                //获取source中的数据
//                JSONObject source = jsonObj.getJSONObject("source");
//                String dbName = source.getString("db");
//                String tableName = source.getString("table");
//                String serverId = jsonObj.getString("server_id");
//                // 当前 JsonObject 是否包含 "after" 字段（表示数据变更后的状态）
//                if (jsonObj.containsKey("after")) {
//                    JSONObject after = jsonObj.getJSONObject("after");
//                    // 将基础信息字段放入新对象中,用于存储提取和整理后的字段
//                    jsonObj.put("ts_ms", tsMs);
//                    jsonObj.put("db", dbName);
//                    jsonObj.put("table", tableName);
//                    jsonObj.put("server_id", serverId);
//                    // 从 after 对象中提取与评论相关的字段
//                    jsonObj.put("appraise", after.getString("appraise"));
//                    jsonObj.put("commentTxt", after.getString("comment_txt"));
//                    jsonObj.put("op", jsonObj.getString("op"));
//                    jsonObj.put("nick_name", jsonObj.getString("nick_name"));
//                    // 提取时间戳和用户相关字段
//                    jsonObj.put("create_time", after.getLong("create_time"));
//                    jsonObj.put("user_id", after.getLong("user_id"));
//                    jsonObj.put("sku_id", after.getLong("sku_id"));
//                    jsonObj.put("id", after.getLong("id"));
//                    jsonObj.put("spu_id", after.getLong("spu_id"));
//                    jsonObj.put("order_id", after.getLong("order_id"));
//                    // 附加维度信息字段
//                    jsonObj.put("dic_name", after.getString("dic_name"));
//                    // 返回整理好的 JSON 对象
//                    return jsonObj;
//                }
//                return null;
//            }
//        });
//        orderCommentMap.print();


        //设置提交
        env.execute();

    }

}
