package com.userportrait;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.JdbcUtils;
import com.stream.common.utils.KafkaUtils;
import com.userportrait.domain.DimBaseCategory;
import com.userportrait.func.AggregateUserDataProcessFunction;
import com.userportrait.func.MapDeviceAndSearchMarkModelFunc;
import com.userportrait.func.ProcessFilterRepeatTsData;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

public class DbusUserInfo6BaseLabel {
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_cdc_db_topic = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String kafka_page_log_topic = ConfigUtils.getString("kafka.page.topic");
    private static final List<DimBaseCategory> dim_base_categories;

    private static final Connection connection;

    private static final double device_rate_weight_coefficient = 0.1; // 设备权重系数
    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数

    //使用jdbc读取三个级别的分类名,返回实体类list
    static {
        try {
            connection = JdbcUtils.getMySQLConnection(
                    ConfigUtils.getString("mysql.url"),
                    ConfigUtils.getString("mysql.user"),
                    ConfigUtils.getString("mysql.pwd"));
            String sql = "select b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     from e_commerce.base_category3 as b3  \n" +
                    "     join e_commerce.base_category2 as b2  \n" +
                    "     on b3.category2_id = b2.id             \n" +
                    "     join e_commerce.base_category1 as b1  \n" +
                    "     on b2.category1_id = b1.id";
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setParallelism(1);

        //读取kafka的ods_initial,设置水位线
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
        //cdc读取kafka的topic_log,设置水位线
        DataStreamSource<String> kafkaCdcLogSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_botstrap_servers,
                        kafka_page_log_topic,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                            if (event != null) {
                                try {
                                    return JSONObject.parseObject(event).getLong("ts");
                                } catch (Exception e) {
                                    //打印异常简要信息,完整调用栈
                                    e.printStackTrace();
                                    System.err.println("Failed to parse event as JSON or get ts: " + event);
                                    //当 JSONObject.parseObject(event) 抛出任何异常（例如格式错误、字段缺失等）时，
                                    // 捕获异常后返回一个默认时间戳 0
                                    return 0L;
                                }
                            }
                            return 0L;
                        }),
                "kafka_cdc_log_source"
        );


        //转为JsonObject类型
        SingleOutputStreamOperator<JSONObject> dataConvertJsonDS = kafkaCdcDbSource.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> dataConvertJsonLogDS = kafkaCdcLogSource.map(JSON::parseObject);

        //map转换,新log的json包含: 设备信息+关键词搜索
        SingleOutputStreamOperator<JSONObject> logDeviceInfoDs = dataConvertJsonLogDS.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                if (jsonObject.containsKey("common") && jsonObject.getJSONObject("common") != null) {
                    JSONObject common = jsonObject.getJSONObject("common");
                    //添加uid字段,如果不存在设置为-1,即未登录用户
                    result.put("uid", common.getString("uid") != null ? common.getString("uid") : "-1");
                    //getLongValue可以提供默认值，避免空指针异常
                    result.put("ts", jsonObject.getLongValue("ts"));
                    //json中嵌套名为deviceInfo的json对象,需要新建一个对象
                    JSONObject deviceInfo = new JSONObject();
                    common.remove("sid");
                    common.remove("mid");
                    common.remove("is_new");
                    deviceInfo.putAll(common);
                    result.put("deviceInfo", deviceInfo);
                    //添加搜索关键词字段
                    if (jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()) {
                        JSONObject pageInfo = jsonObject.getJSONObject("page");
                        if (pageInfo.containsKey("item_type") && pageInfo.getString("item_type").equals("keyword")) {
                            String item = pageInfo.getString("item");
                            result.put("search_item", item);
                        }
                    }
                }
                //deviceInfo中添加os字段
                JSONObject deviceInfo = result.getJSONObject("deviceInfo");
                String os = deviceInfo.getString("os").split(" ")[0];
                deviceInfo.put("os", os);
                return result;
            }
        });

        //过滤log中uid为空的字段
        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = logDeviceInfoDs.filter(data -> !data.getString("uid").isEmpty());

        //log按uid分组
        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(data -> data.getString("uid"));

        //去除log中重复的json数据
        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsData());


        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(data -> data.getString("uid"))
                //将每个用户数据进行聚合,每个参数的值都设置为set
                .process(new AggregateUserDataProcessFunction())
                //每个 2 分钟窗口内的所有数据，最终只保留最后一条。
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2);

        //设备评分模型
        //参数为三级分类的汇总表的实体类list,设备权重系数,搜索权重系数
        //{"uid":"39","os":"iOS,Android","ch":"Appstore,oppo","pv":3,"md":"iPhone 13,Redmi k50","search_item":"","ba":"iPhone,Redmi"}
        //{"device_35_39":0.04,"os":"iOS,Android","device_50":0.02,"search_25_29":0,"ch":"Appstore,xiaomi,oppo,wandoujia","pv":9,"device_30_34":0.05,"device_18_24":0.07,"search_50":0,"search_40_49":0,"uid":"10","device_25_29":0.06,"md":"xiaomi 13,realme Neo2,iPhone 14 Plus,xiaomi 13 Pro ,Redmi k50","search_18_24":0,"judge_os":"iOS","search_35_39":0,"device_40_49":0.03,"search_item":"","ba":"iPhone,xiaomi,realme,Redmi","search_30_34":0}
        SingleOutputStreamOperator<JSONObject> deviceAndSearchMarkDS = win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));


        //提取user_id表
        SingleOutputStreamOperator<JSONObject> filterUserInfoDS = dataConvertJsonDS.filter(data -> data.getJSONObject("source").getString("table").equals("user_info"));

        //用户表不会删除只会更新,提取after
        SingleOutputStreamOperator<JSONObject> finalUserInfoDS = filterUserInfoDS.map(new RichMapFunction<JSONObject, JSONObject>() {
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
        
        //提取user_info表数据,并补充年龄年代星座字段
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



        //按uid分组,准备进行join
        KeyedStream<JSONObject, String> keyedStreamUserInfoDs = filterUserInfoDs.keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedStreamUserInfoSupDs = filterUserInfoSupDs.keyBy(data -> data.getString("uid"));

        //使用intervalJoin进行join
        //有一个问题是,kafka中的数据是8号9号生成的,但是user_info_sup_msg表中的数据是13号生成的,设置间隔为7天试试
        SingleOutputStreamOperator<JSONObject> processIntervalJoinUserInfo6BaseMessageDs =
                keyedStreamUserInfoDs.intervalJoin(keyedStreamUserInfoSupDs)
                .between(Time.days(-7),Time.days(7))
                .process(
                        new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                            @Override
                            public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                                JSONObject result = new JSONObject();
                                if (jsonObject.getString("uid").equals(jsonObject2.getString("uid"))) {
                                    result.putAll(jsonObject);
                                    result.put("height", jsonObject2.getString("height"));
                                    result.put("unit_height", jsonObject2.getString("unit_height"));
                                    result.put("weight", jsonObject2.getString("weight"));
                                    result.put("unit_weight", jsonObject2.getString("unit_weight"));
                                }
                                collector.collect(result);
                            }
                        }
                );




        //过滤出order_info表
        SingleOutputStreamOperator<JSONObject> filterOrderInfoDS = dataConvertJsonDS.filter(data -> data.getJSONObject("source").getString("table").equals("order_info"));
        //过滤出order_detail表
        SingleOutputStreamOperator<JSONObject> filterOrderDetailDS = dataConvertJsonDS.filter(data -> data.getJSONObject("source").getString("table").equals("order_detail"));

        //对于order_info进行去重
        SingleOutputStreamOperator<JSONObject> processStageOrderInfoDs = filterOrderInfoDS
                .keyBy(data -> "all")
                .process(new ProcessFilterRepeatTsData());





        //过滤掉order_info的脏数据,即我只要最新的order_id的数据,然后与order_detail进行intervalJoin
        //获取到order_info 订单id,用户id,创建时间,order_detail的sku_id,order_price
        //根据sku_id,通过jdbc连接mysql,sku_info的spu_id,spu_info的c3_id,tm_id,连接c3,c2,c1,category_compare_dic2的order_category,base_trademark中的品牌名
        //然后根据use_id分组,把order_category,order_price,品牌名进行计数,获取投入最大的价格和品类和品牌
        //然后进行打分
        SingleOutputStreamOperator<JSONObject> latestOrderInfoProcessDS = processStageOrderInfoDs
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    //取到after字段,并添加ts字段
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        JSONObject result = new JSONObject();

                        if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null && !jsonObject.getJSONObject("after").isEmpty()) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            result.putAll(after);
                            result.put("ts_ms", jsonObject.getString("ts_ms"));
                        }
                        return result;
                    }
                })
                .filter(data -> data.containsKey("id") && data.getString("id") != null)
                //根据订单id分区,设置状态算子,取到同一订单最新的数据
                .keyBy(data -> data.getString("id"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<JSONObject> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> descriptor = new ValueStateDescriptor<>("valueState", JSONObject.class);
                        valueState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject value = valueState.value();
                        if (value == null) {
                            valueState.update(jsonObject);
                            collector.collect(jsonObject);
                        } else {
                            if (value.getLong("ts_ms") < jsonObject.getLong("ts_ms")) {
                                valueState.update(jsonObject);
                                collector.collect(jsonObject);
                            }
                        }
                    }
                });


        //提取order_detail表中的字段order_id,sku_id,order_price
        //并根据order_id进行分组
        KeyedStream<JSONObject, String> orderDetailKeyDS = filterOrderDetailDS.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    result.put("order_id", after.getString("order_id"));
                    result.put("sku_id", after.getString("sku_id"));
                    result.put("order_price", after.getDouble("order_price"));

                }
                return result;
            }
        })
                .filter(data -> data.containsKey("order_id") && data.getString("order_id") != null && !data.getString("order_id").isEmpty())
                .keyBy(data -> data.getString("order_id"));



        //order_info中获取订单id,用户id,创建时间
        //与order_detail进行intervalJoin
        SingleOutputStreamOperator<JSONObject> orderInfoJoinOrderDetailDS = latestOrderInfoProcessDS.map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        JSONObject result = new JSONObject();
                        result.put("order_id", jsonObject.getString("id"));
                        result.put("order_time", jsonObject.getLongValue("create_time"));
                        result.put("user_id", jsonObject.getString("user_id"));
                        return result;
                    }
                })
                .filter(data -> data.containsKey("order_id") && data.getString("order_id") != null && !data.getString("order_id").isEmpty())
                .keyBy(data -> data.getString("order_id"))
                .intervalJoin(orderDetailKeyDS)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObj1, JSONObject jsonObj2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject result = new JSONObject();
                        if (jsonObj1.get("order_id").equals(jsonObj2.get("order_id"))) {
                            result.putAll(jsonObj1);
                            result.putAll(jsonObj2);
                            result.put("order_time", Instant.ofEpochMilli(jsonObj1.getLong("order_time")).atZone(ZoneId.systemDefault()).getHour());
                        }
                        collector.collect(result);
                    }
                });

        //根据sku_id,通过jdbc连接mysql
        // sku_info的spu_id,
        // spu_info的c3_id,tm_id,
        // 连接c3,c2,c1,
        // category_compare_dic2的order_category,
        // base_trademark中的品牌名
        orderInfoJoinOrderDetailDS
                .keyBy(data -> data.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                })
                .







        //设备评分和关键词评分输出
//        deviceAndSearchMarkDS.print();
        //六个基础标签输出
//        processIntervalJoinUserInfo6BaseMessageDs.print();


        env.execute();
    }

}
