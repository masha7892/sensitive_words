package com.realtime;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.realtime.stream.CommonGenerateTempLate;
import com.realtime.util.AsyncHbaseDimBaseDicFunc;
import com.realtime.util.KafkaOffsetUtils;
import com.realtime.util.SensitiveWordsUtils;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import sun.reflect.misc.ConstructorUtil;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

//keypoint 评论数据的敏感词过滤,这段代码主要是ai生成评论,替换原有评论,然后随机添加敏感词到评论中,发送到kafka
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
        //报错Insufficient number of network buffers: required 33, but only 2 available,设置并行度后解决,可能是默认并行度的问题
        env.setParallelism(1);




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

//        kafkaCdcDbSource.print();



//        //过滤出订单主表
        SingleOutputStreamOperator<JSONObject> filteredOrderInfoStream = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(jsonObj -> jsonObj.getJSONObject("source").getString("table").equals("order_info"));
//        filteredOrderInfoStream.print();



//        //过滤出订单表,并且使用appraise分组
//        //经过keyBy的流不直接支持 print()
        KeyedStream<JSONObject, String> filteredCommentInfoStream = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(jsonObj -> jsonObj.getJSONObject("source").getString("table").equals("comment_info"))
                .keyBy(jsonObj -> jsonObj.getJSONObject("after").getString("appraise"));

//        //异步io到appraise对应的dic_name
        SingleOutputStreamOperator<JSONObject> enrichedStream = AsyncDataStream
                .unorderedWait(
                        filteredCommentInfoStream,
                        new AsyncHbaseDimBaseDicFunc(),
                        60,
                        TimeUnit.SECONDS,
                        1000
                );

//        enrichedStream.print();

//        //map操作生成新的jsonObj,用于存储comment_info提取和整理后的字段
        SingleOutputStreamOperator<JSONObject> orderCommentMap = enrichedStream.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject  jsonObj) throws Exception {
                //获取ts数据
                JSONObject jsonObject = new JSONObject();
                Long tsMs = jsonObj.getLong("ts_ms");
                //获取source中的数据
                JSONObject source = jsonObj.getJSONObject("source");
                String dbName = source.getString("db");
                String tableName = source.getString("table");
                String serverId = source.getString("server_id");
                // 当前 JsonObj 是否包含 "after" 字段（表示数据变更后的状态）
                if (jsonObj.containsKey("after")) {
                    JSONObject after = jsonObj.getJSONObject("after");
                    // 将基础信息字段放入新对象中,用于存储提取和整理后的字段
                    jsonObject.put("ts_ms", tsMs);
                    jsonObject.put("db", dbName);
                    jsonObject.put("table", tableName);
                    jsonObject.put("server_id", serverId);
                    // 从 after 对象中提取与评论相关的字段
                    jsonObject.put("appraise", after.getString("appraise"));
                    jsonObject.put("commentTxt", after.getString("comment_txt"));
                    jsonObject.put("op", jsonObj.getString("op"));
                    jsonObject.put("nick_name", after.getString("nick_name"));
                    // 提取时间戳和用户相关字段
                    jsonObject.put("create_time", after.getLong("create_time"));
                    jsonObject.put("user_id", after.getLong("user_id"));
                    jsonObject.put("sku_id", after.getLong("sku_id"));
                    jsonObject.put("id", after.getLong("id"));
                    jsonObject.put("spu_id", after.getLong("spu_id"));
                    jsonObject.put("order_id", after.getLong("order_id"));
                    // 附加维度信息字段
                    jsonObject.put("dic_name", after.getString("dic_name"));
                    // 返回整理好的 JSON 对象
                    return jsonObject;
                }
                return null;
            }
        });
//        orderCommentMap.print();


        //处理订单主表的数据
        SingleOutputStreamOperator<JSONObject> orderInfoMapDs = filteredOrderInfoStream.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject inputJsonObj) throws Exception {
                //有op字段,则使用op,没有则使用默认值
                String op = inputJsonObj.containsKey("op")
                        ? inputJsonObj.getString("op")
                        : "没有op字段";
                String ts_ms = inputJsonObj.containsKey("ts_ms")
                        ? inputJsonObj.getString("ts_ms")
                        : "没有ts字段";
                JSONObject dataObj;
                //这里当没有before字段时,inputJsonObj.getJSONObject("before")就会报空指针异常
                if (inputJsonObj.containsKey("after") && inputJsonObj.getJSONObject("after") != null && !inputJsonObj.getJSONObject("after").isEmpty()) {
                    dataObj = inputJsonObj.getJSONObject("after");
                } else if (inputJsonObj.containsKey("before")  && inputJsonObj.getJSONObject("before") != null && !inputJsonObj.getJSONObject("before").isEmpty()){
                    dataObj = inputJsonObj.getJSONObject("before");
                }else {
                    dataObj  = new JSONObject();
                }
                JSONObject resultJsonObj = new JSONObject();
                resultJsonObj.put("op", op);
                resultJsonObj.put("ts_ms", ts_ms);
                resultJsonObj.putAll(dataObj);
                return resultJsonObj;
            }
        });
//        orderInfoMapDs.print();


        //评论表和订单表分组,intervalJoin 只能作用于 KeyedStream
        KeyedStream<JSONObject, String> keyedOrderCommentStream = orderCommentMap.keyBy(data -> data.getString("order_id"));
        KeyedStream<JSONObject, String> keyedOrderInfoStream = orderInfoMapDs.keyBy(data -> data.getString("id"));

        //评论表和订单表关联,窗口大小为1分钟,允许迟到1分钟,
        SingleOutputStreamOperator<JSONObject> orderMsgAllDs = keyedOrderCommentStream.intervalJoin(keyedOrderInfoStream)
                .between(Time.minutes(-1), Time.minutes(1))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject comment, JSONObject info, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        //.clone() 创建了一个 JSONObject 对象的浅拷贝，目的是避免对原始 comment 对象的直接修改
                        //由于 clone() 方法返回的是 Object 类型，因此需要显式地将其转换为 JSONObject 类型
                        JSONObject cloneComment = (JSONObject) comment.clone();
                        for (String key : info.keySet()) {
                            cloneComment.put("info_" + key, info.get(key));
                        }
                        collector.collect(cloneComment);
                    }
                });
//        orderMsgAllDs.print();

        // 通过AI 生成评论数据，`Deepseek 7B` 模型即可
        SingleOutputStreamOperator<JSONObject> supplementDataMap = orderMsgAllDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                jsonObject.put("commentTxt", CommonGenerateTempLate.GenerateComment(jsonObject.getString("dic_name"), jsonObject.getString("info_trade_body")));
                return jsonObject;
            }
        });
//        supplementDataMap.print();

        SingleOutputStreamOperator<JSONObject> suppleMapDs = supplementDataMap.map(new RichMapFunction<JSONObject, JSONObject>() {
            //初始化一个随机数实例
            private transient Random random;

            @Override
            public void open(Configuration parameters) throws Exception {
                random = new Random();
            }

            //生成一个 0 到 1 之间的随机数，如果该数小于 0.2（即 20% 的概率），则：
            //从 sensitiveWordsLists 中随机选取一个敏感词。
            //将该敏感词追加到原有的 commentTxt 字段中，使用逗号分隔。
            //输出修改后的 JSONObject 到标准错误流，便于调试。
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                if (random.nextDouble() < 0.2) {
                    jsonObject.put("commentTxt", jsonObject.getString("commentTxt") + "," + SensitiveWordsUtils.getRandomElement(sensitiveWordsLists));
                    System.err.println("change commentTxt: " + jsonObject);
                }
                return jsonObject;
            }
        });

//        suppleMapDs.print();

        //在每个 JSONObject 中添加一个新的字段 ds，其值为 ts_ms 字段表示的时间戳转换为的日期字符串，格式为 yyyyMMdd
        SingleOutputStreamOperator<JSONObject> suppleTimeFieldDs = suppleMapDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                jsonObject.put("ds", new SimpleDateFormat("yyyyMMdd").format(new Date(jsonObject.getLong("ts_ms"))));
                return jsonObject;
            }
        });
        suppleTimeFieldDs.print();

//        suppleTimeFieldDs.map(js ->js.toJSONString()).sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers,kafka_db_fact_comment_topic));
        //设置提交
        env.execute();

    }

}
