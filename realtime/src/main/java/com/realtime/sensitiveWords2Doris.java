package com.realtime;

import com.alibaba.fastjson.JSONObject;
import com.realtime.util.FlinkSinkUtil;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;

//敏感词写入doris
public class sensitiveWords2Doris {
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_result_sensitive_words_topic = ConfigUtils.getString("kafka.result.sensitive.words.topic");

    @SneakyThrows
    public static void main(String[] args) {

        //设置环境,读取kafka
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_botstrap_servers,
                        kafka_result_sensitive_words_topic,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "kafka_cdc_db_source"
        );
        //过滤掉非敏感词数据
        SingleOutputStreamOperator<String> isViolation = kafkaCdcDbSource.filter(data -> JSONObject.parseObject(data).getString("is_violation").equals("1"));
        //将json字符串转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = isViolation.map(new MapFunction<String, JSONObject>() {

            @Override
            public JSONObject map(String s) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);

                    JSONObject jsonObj = new JSONObject();
                    //名字
                    String consignee = jsonObject.getString("consignee");
                    //等级P0
                    String violation_grade = jsonObject.getString("violation_grade");
                    //敏感词
                    String violation_msg = jsonObject.getString("violation_msg");
                    jsonObj.put("consignee", consignee);
                    jsonObj.put("violation_grade", violation_grade);
                    jsonObj.put("violation_msg", violation_msg);
                    return jsonObj;

                } catch (Exception e) {
                    System.err.println("数据转换异常：" + e.getMessage());
                    return new JSONObject();
                }

            }
        });
        jsonObjDS.print();
        jsonObjDS.map(jsonStr->JSONObject.toJSONString(jsonStr))
                .sinkTo(FlinkSinkUtil.getDorisSink("sensitive_words"));

        env.execute();
    }


}
