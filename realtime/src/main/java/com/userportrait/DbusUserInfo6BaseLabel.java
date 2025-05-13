package com.userportrait;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DbusUserInfo6BaseLabel {
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_cdc_db_topic = ConfigUtils.getString("kafka.cdc.db.topic");

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        //读取kafka,设置水位线
        DataStreamSource<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_botstrap_servers,
                        kafka_cdc_db_topic,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                            if (event != null) {
                                try {
                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                } catch (Exception e) {
                                    //打印异常简要信息,完整调用栈
                                    e.printStackTrace();
                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                    //当 JSONObject.parseObject(event) 抛出任何异常（例如格式错误、字段缺失等）时，
                                    // 捕获异常后返回一个默认时间戳 0
                                    return 0L;
                                }
                            }
                            return 0L;
                        }),
                "kafka_cdc_db_source"
        );
//        kafkaCdcDbSource.print();

        //转为JsonObject类型
        SingleOutputStreamOperator<JSONObject> dataConvertJsonDS = kafkaCdcDbSource.map(JSON::parseObject);

        //提取user_id表
        SingleOutputStreamOperator<JSONObject> filterOrderInfoDS = dataConvertJsonDS.filter(data -> data.getJSONObject("source").getString("table").equals("user_info"));

        //用户表不会删除只会更新,提取after
        SingleOutputStreamOperator<JSONObject> finalUserInfoDS = filterOrderInfoDS.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    if (after.containsKey("birthday") && after.getString("birthday") != null) {
                        Long epochDay = after.getLong("birthday");
                        if (epochDay != null) {
                            //日期差值转为年月日
                            LocalDate date = LocalDate.ofEpochDay(epochDay);
                            after.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                        }
                    }
                }
                return jsonObject;
            }
        });

        //过滤出user_info_sup_msg表
        SingleOutputStreamOperator<JSONObject> filterUserInfoSupMsgDS = dataConvertJsonDS.filter(data -> data.getJSONObject("source").getString("table").equals("user_info_sup_msg"));
        
        //补充user_info表数据以及年龄年代星座字段
        SingleOutputStreamOperator<JSONObject> mapUserInfoDs = finalUserInfoDS.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                    //获取after
                    JSONObject after = jsonObject.getJSONObject("after");
                    //获取after中的字段
                    result.put("uid", after.getString("id"));
                    result.put("uname", after.getString("name"));
                    result.put("user_level", after.getString("user_level"));
                    result.put("login_name", after.getString("login_name"));
                    result.put("phone_num", after.getString("phone_num"));
                    result.put("email", after.getString("email"));
                    result.put("gender", after.getString("gender") != null ? after.getString("gender") : "home");
                    result.put("birthday", after.getString("birthday"));
                    result.put("ts_ms", jsonObject.getLongValue("ts_ms"));
                    //添加年代,年龄,星座字段
                    String birthdayStr = after.getString("birthday");
                    //既不是 null 也不是空字符串
                    if (birthdayStr != null && !birthdayStr.isEmpty()) {
                        try {
                            //将日期差值解析为年月日
                            LocalDate birthday = LocalDate.parse(birthdayStr, DateTimeFormatter.ISO_DATE);
                            //获取该时区的时间
                            LocalDate currentDate = LocalDate.now(ZoneId.of("Asia/Shanghai"));
                            //获取年龄
                            int age = Period.between(birthday, currentDate).getYears();
                            //获取年代
                            int decade = birthday.getYear() / 10 * 10;
                            //获取星座
                            int month = birthday.getMonthValue();
                            int day = birthday.getDayOfMonth();
                            // 星座日期范围定义
                            String zodiacSign;
                            if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) zodiacSign = "摩羯座";
                            else if (month == 1 || month == 2 && day <= 18) zodiacSign = "水瓶座";
                            else if (month == 2 || month == 3 && day <= 20) zodiacSign = "双鱼座";
                            else if (month == 3 || month == 4 && day <= 19) zodiacSign = "白羊座";
                            else if (month == 4 || month == 5 && day <= 20) zodiacSign = "金牛座";
                            else if (month == 5 || month == 6 && day <= 21) zodiacSign = "双子座";
                            else if (month == 6 || month == 7 && day <= 22) zodiacSign = "巨蟹座";
                            else if (month == 7 || month == 8 && day <= 22) zodiacSign = "狮子座";
                            else if (month == 8 || month == 9 && day <= 22) zodiacSign = "处女座";
                            else if (month == 9 || month == 10 && day <= 23) zodiacSign = "天秤座";
                            else if (month == 10 || month == 11 && day <= 22) zodiacSign = "天蝎座";
                            else zodiacSign = "射手座";
                            //补充字段
                            result.put("age", age);
                            result.put("decade", decade);
                            result.put("zodiac_sign", zodiacSign);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
                return result;
            }
        });

        //处理user_info_sup_msg表
        SingleOutputStreamOperator<JSONObject> mapUserInfoSupDs = filterUserInfoSupMsgDS.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    result.put("uid", after.getString("uid"));
                    result.put("unit_height", after.getString("unit_height"));
                    result.put("create_ts", after.getLong("create_ts"));
                    result.put("weight", after.getString("weight"));
                    result.put("unit_weight", after.getString("unit_weight"));
                    result.put("height", after.getString("height"));
                    result.put("ts_ms", jsonObject.getLong("ts_ms"));
                }
                return result;
            }
        });

        //过滤掉mapUserInfoDs和mapUserInfoSupDs中uid为空的数据
        SingleOutputStreamOperator<JSONObject> filterUserInfoDs = mapUserInfoDs.filter(jsonObject -> jsonObject.containsKey("uid") && !jsonObject.getString("uid").isEmpty());
        SingleOutputStreamOperator<JSONObject> filterUserInfoSupDs = mapUserInfoSupDs.filter(jsonObject -> jsonObject.containsKey("uid") && !jsonObject.getString("uid").isEmpty());

        KeyedStream<JSONObject, String> keyedStreamUserInfoDs = filterUserInfoDs.keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedStreamUserInfoSupDs = filterUserInfoSupDs.keyBy(data -> data.getString("uid"));
        keyedStreamUserInfoDs.intervalJoin(keyedStreamUserInfoSupDs)
                .between(Time.minutes(-5), Time.minutes(5))
                .process(
                        new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                            @Override
                            public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                                JSONObject result = new JSONObject();
                                if (jsonObject.getString("uid").equals(jsonObject2.getString("uid"))){

                                }
                            }
                        }
                )

        env.execute();
    }

}
