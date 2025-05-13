package com.realtime;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.github.houbb.sensitive.word.core.SensitiveWordHelper;
import com.realtime.func.FilterBloomDeduplicatorFunc;
import com.realtime.func.MapCheckRedisSensitiveWordsFunc;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;
import java.util.List;
public class DbusBanBlackListUserInfo2Kafka {

    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_db_fact_comment_topic = ConfigUtils.getString("kafka.db.fact.comment.topic");
    private static final String kafka_result_sensitive_words_topic = ConfigUtils.getString("kafka.result.sensitive.words.topic");

    @SneakyThrows
    public static void main(String[] args) {

        //设置环境,读取kafka
        System.setProperty("HADOOP_USER_NAME","hdfs");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_botstrap_servers,
                        kafka_db_fact_comment_topic,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "kafka_cdc_db_source"
        );



        //转为jsonobject格式
        SingleOutputStreamOperator<JSONObject> mapJsonStr = kafkaCdcDbSource.map(JSON::parseObject);

        //根据 order_id 对数据流进行分组，确保每个订单的数据被单独处理。
        //布隆过滤器去重,内部实现了布隆过滤器逻辑，用于判断当前数据是否已经出现过
        //1000000：布隆过滤器的预期容量，即预计处理的数据条数。
        //0.01：允许的误判率，即有 1% 的概率将未出现的数据误判为已存在。
        SingleOutputStreamOperator<JSONObject> bloomFilterDs = mapJsonStr.keyBy(data -> data.getLong("order_id"))
                .filter(new FilterBloomDeduplicatorFunc(1000000, 0.01));

//        bloomFilterDs.print();

        //这里是取到逗号分隔后最后一个关键词,判断关键词是否存在于读取redis中键为sensitive_words的set中
        //如果存在设置P0
        SingleOutputStreamOperator<JSONObject> SensitiveWordsDs = bloomFilterDs.map(new MapCheckRedisSensitiveWordsFunc());

//        SensitiveWordsDs.print();


        SingleOutputStreamOperator<JSONObject> secondCheckMap = SensitiveWordsDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                //读取未违规数据进行二次检查。
                if (jsonObject.getIntValue("is_violation") == 0) {
                    String msg = jsonObject.getString("msg");
                    //在指定文本 msg 中查找所有匹配的敏感词，并返回包含这些敏感词的集合
                    //这里可能要设置SensitiveWordHelper的词库,先不写了
                    List<String> msgSen = SensitiveWordHelper.findAll(msg);
                    if (msgSen.size() > 0) {
                        jsonObject.put("violation_grade", "P1");
                        jsonObject.put("violation_msg", String.join(", ", msgSen));
                    }
                }
                return jsonObject;
            }
        });

//        secondCheckMap.print();
        //写入kafka中
        secondCheckMap.map(data -> data.toJSONString())
                        .sinkTo(
                                KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_result_sensitive_words_topic)
                        );


        env.execute();
    }
}
