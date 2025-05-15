package com.userportrait;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.JdbcUtils;
import com.stream.common.utils.KafkaUtils;
import com.userportrait.domain.DimBaseCategory;
import com.userportrait.func.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.Connection;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

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
//        {"before":null,"after":{"id":192,"uid":192,"gender":null,"height":"175","unit_height":"cm","weight":"73","unit_weight":"kg","flag":null,"create_ts":1747161408000},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1747132608000,"snapshot":"false","db":"e_commerce","sequence":null,"table":"user_info_sup_msg","server_id":1,"gtid":null,"file":"mysql-bin.000029","pos":24454,"row":191,"thread":null,"query":null},"op":"c","ts_ms":1747132603683,"transaction":null}
//        {"before":null,"after":{"id":193,"uid":193,"gender":"M","height":"186","unit_height":"cm","weight":"67","unit_weight":"kg","flag":null,"create_ts":1747161408000},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1747132608000,"snapshot":"false","db":"e_commerce","sequence":null,"table":"user_info_sup_msg","server_id":1,"gtid":null,"file":"mysql-bin.000029","pos":24454,"row":192,"thread":null,"query":null},"op":"c","ts_ms":1747132603683,"transaction":null}
//        {"before":null,"after":{"id":194,"uid":194,"gender":null,"height":"159","unit_height":"cm","weight":"73","unit_weight":"kg","flag":null,"create_ts":1747161408000},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1747132608000,"snapshot":"false","db":"e_commerce","sequence":null,"table":"user_info_sup_msg","server_id":1,"gtid":null,"file":"mysql-bin.000029","pos":24454,"row":193,"thread":null,"query":null},"op":"c","ts_ms":1747132603683,"transaction":null}
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
//        {"common":{"ar":"17","uid":"69","os":"iOS 13.2.3","ch":"Appstore","is_new":"0","md":"iPhone 14 Plus","mid":"mid_380","vc":"v2.1.134","ba":"iPhone","sid":"57def5f8-55ba-4d65-92ea-34cdee38ed6d"},"page":{"page_id":"order_list","during_time":17224,"last_page_id":"mine"},"ts":1746757085000}
//        {"common":{"ar":"23","uid":"42","os":"Android 13.0","ch":"wandoujia","is_new":"1","md":"realme Neo2","mid":"mid_390","vc":"v2.1.132","ba":"realme","sid":"90c4a31a-ce52-4042-96e7-f340c92985b4"},"page":{"from_pos_seq":5,"page_id":"good_detail","item":"10","during_time":12969,"item_type":"sku_id","last_page_id":"good_detail","from_pos_id":4},"ts":1746757086000}
//        {"common":{"ar":"23","uid":"42","os":"Android 13.0","ch":"wandoujia","is_new":"1","md":"realme Neo2","mid":"mid_390","vc":"v2.1.132","ba":"realme","sid":"90c4a31a-ce52-4042-96e7-f340c92985b4"},"page":{"page_id":"cart","during_time":17582,"last_page_id":"good_detail"},"ts":1746757086000}
//        {"common":{"ar":"23","uid":"42","os":"Android 13.0","ch":"wandoujia","is_new":"1","md":"realme Neo2","mid":"mid_390","vc":"v2.1.132","ba":"realme","sid":"90c4a31a-ce52-4042-96e7-f340c92985b4"},"page":{"page_id":"order","item":"1,3,35,11,10","during_time":11466,"item_type":"sku_ids","last_page_id":"cart"},"ts":1746757088000}
//        {"common":{"ar":"23","uid":"42","os":"Android 13.0","ch":"wandoujia","is_new":"1","md":"realme Neo2","mid":"mid_390","vc":"v2.1.132","ba":"realme","sid":"90c4a31a-ce52-4042-96e7-f340c92985b4"},"page":{"page_id":"payment","item":"379","during_time":10895,"item_type":"order_id","last_page_id":"order"},"ts":1746757088000}
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
//        {"uid":"69","deviceInfo":{"ar":"17","uid":"69","os":"iOS","ch":"Appstore","md":"iPhone 14 Plus","vc":"v2.1.134","ba":"iPhone"},"ts":1746757085000}
//        {"uid":"42","deviceInfo":{"ar":"23","uid":"42","os":"Android","ch":"wandoujia","md":"realme Neo2","vc":"v2.1.132","ba":"realme"},"ts":1746757086000}
//        {"uid":"42","deviceInfo":{"ar":"23","uid":"42","os":"Android","ch":"wandoujia","md":"realme Neo2","vc":"v2.1.132","ba":"realme"},"ts":1746757086000}
//        {"uid":"42","deviceInfo":{"ar":"23","uid":"42","os":"Android","ch":"wandoujia","md":"realme Neo2","vc":"v2.1.132","ba":"realme"},"ts":1746757088000}
//        {"uid":"42","deviceInfo":{"ar":"23","uid":"42","os":"Android","ch":"wandoujia","md":"realme Neo2","vc":"v2.1.132","ba":"realme"},"ts":1746757088000}
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


//        {"uid":"15","os":"iOS,Android","ch":"Appstore,oppo","pv":4,"md":"iPhone 14 Plus,SAMSUNG Galaxy s22","search_item":"","ba":"iPhone,SAMSUNG","ts":1746663197000}
//        {"uid":"50","os":"iOS,Android","ch":"Appstore,xiaomi","pv":5,"md":"iPhone 14,vivo IQOO Z6x ","search_item":"","ba":"iPhone,vivo","ts":1746757073000}
//        {"uid":"28","os":"Android","ch":"oppo,wandoujia,360","pv":8,"md":"xiaomi 13,vivo x90,Redmi k50","search_item":"","ba":"xiaomi,vivo,Redmi","ts":1746663197000}
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


        //过滤出user_info表
//        {"op":"c","after":{"birthday":13826,"gender":"F","create_time":1746785884000,"login_name":"v6v8984bh","nick_name":"莲真","name":"戚莲真","user_level":"1","phone_num":"13672116842","id":195,"email":"v6v8984bh@hotmail.com"},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":951055,"name":"mysql_binlog_source","row":0,"ts_ms":1746757084000,"snapshot":"false","db":"e_commerce","table":"user_info"},"ts_ms":1746757081114}
//        {"op":"c","after":{"birthday":10600,"gender":"M","create_time":1746785884000,"login_name":"rc76n9p25i6","nick_name":"有坚","name":"司马有坚","user_level":"1","phone_num":"13335669677","id":196,"email":"rc76n9p25i6@gmail.com"},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":982752,"name":"mysql_binlog_source","row":0,"ts_ms":1746757084000,"snapshot":"false","db":"e_commerce","table":"user_info"},"ts_ms":1746757081350}
//        {"op":"c","after":{"birthday":13216,"create_time":1746785885000,"login_name":"tjcklra6cq2f","nick_name":"阿光","name":"鲁光","user_level":"1","phone_num":"13948471483","id":197,"email":"tjcklra6cq2f@163.com"},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":1055015,"name":"mysql_binlog_source","row":0,"ts_ms":1746757085000,"snapshot":"false","db":"e_commerce","table":"user_info"},"ts_ms":1746757082230}
//        {"op":"c","after":{"birthday":10965,"gender":"F","create_time":1746785885000,"login_name":"btxahcagy","nick_name":"莲莲","name":"戚莲","user_level":"1","phone_num":"13125455788","id":198,"email":"btxahcagy@yahoo.com.cn"},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":1068917,"name":"mysql_binlog_source","row":0,"ts_ms":1746757085000,"snapshot":"false","db":"e_commerce","table":"user_info"},"ts_ms":1746757082559}
//        {"op":"c","after":{"birthday":9047,"create_time":1746785886000,"login_name":"ohcy5y","nick_name":"阿永","name":"毕永","user_level":"2","phone_num":"13555179861","id":199,"email":"ohcy5y@0355.net"},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":1100901,"name":"mysql_binlog_source","row":0,"ts_ms":1746757086000,"snapshot":"false","db":"e_commerce","table":"user_info"},"ts_ms":1746757083271}
        SingleOutputStreamOperator<JSONObject> filterUserInfoDS = dataConvertJsonDS.filter(data -> data.getJSONObject("source").getString("table").equals("user_info"));

        //用户表不会删除只会更新,提取after
//        {"op":"c","after":{"birthday":"2007-11-09","gender":"F","create_time":1746785884000,"login_name":"v6v8984bh","nick_name":"莲真","name":"戚莲真","user_level":"1","phone_num":"13672116842","id":195,"email":"v6v8984bh@hotmail.com"},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":951055,"name":"mysql_binlog_source","row":0,"ts_ms":1746757084000,"snapshot":"false","db":"e_commerce","table":"user_info"},"ts_ms":1746757081114}
//        {"op":"c","after":{"birthday":"1999-01-09","gender":"M","create_time":1746785884000,"login_name":"rc76n9p25i6","nick_name":"有坚","name":"司马有坚","user_level":"1","phone_num":"13335669677","id":196,"email":"rc76n9p25i6@gmail.com"},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":982752,"name":"mysql_binlog_source","row":0,"ts_ms":1746757084000,"snapshot":"false","db":"e_commerce","table":"user_info"},"ts_ms":1746757081350}
//        {"op":"c","after":{"birthday":"2006-03-09","create_time":1746785885000,"login_name":"tjcklra6cq2f","nick_name":"阿光","name":"鲁光","user_level":"1","phone_num":"13948471483","id":197,"email":"tjcklra6cq2f@163.com"},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":1055015,"name":"mysql_binlog_source","row":0,"ts_ms":1746757085000,"snapshot":"false","db":"e_commerce","table":"user_info"},"ts_ms":1746757082230}
//        {"op":"c","after":{"birthday":"2000-01-09","gender":"F","create_time":1746785885000,"login_name":"btxahcagy","nick_name":"莲莲","name":"戚莲","user_level":"1","phone_num":"13125455788","id":198,"email":"btxahcagy@yahoo.com.cn"},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":1068917,"name":"mysql_binlog_source","row":0,"ts_ms":1746757085000,"snapshot":"false","db":"e_commerce","table":"user_info"},"ts_ms":1746757082559}
//        {"op":"c","after":{"birthday":"1994-10-09","create_time":1746785886000,"login_name":"ohcy5y","nick_name":"阿永","name":"毕永","user_level":"2","phone_num":"13555179861","id":199,"email":"ohcy5y@0355.net"},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":1100901,"name":"mysql_binlog_source","row":0,"ts_ms":1746757086000,"snapshot":"false","db":"e_commerce","table":"user_info"},"ts_ms":1746757083271}
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
//        {"op":"c","after":{"uid":196,"flag":"change","gender":"M","unit_height":"cm","create_ts":1747161408000,"weight":"64","id":196,"unit_weight":"kg","height":"159"},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000029","connector":"mysql","pos":24454,"name":"mysql_binlog_source","row":195,"ts_ms":1747132608000,"snapshot":"false","db":"e_commerce","table":"user_info_sup_msg"},"ts_ms":1747132603683}
//        {"op":"c","after":{"uid":197,"unit_height":"cm","create_ts":1747161408000,"weight":"47","id":197,"unit_weight":"kg","height":"185"},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000029","connector":"mysql","pos":24454,"name":"mysql_binlog_source","row":196,"ts_ms":1747132608000,"snapshot":"false","db":"e_commerce","table":"user_info_sup_msg"},"ts_ms":1747132603683}
//        {"op":"c","after":{"uid":198,"gender":"F","unit_height":"cm","create_ts":1747161408000,"weight":"53","id":198,"unit_weight":"kg","height":"155"},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000029","connector":"mysql","pos":24454,"name":"mysql_binlog_source","row":197,"ts_ms":1747132608000,"snapshot":"false","db":"e_commerce","table":"user_info_sup_msg"},"ts_ms":1747132603683}
//        {"op":"c","after":{"uid":199,"unit_height":"cm","create_ts":1747161408000,"weight":"75","id":199,"unit_weight":"kg","height":"159"},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000029","connector":"mysql","pos":24454,"name":"mysql_binlog_source","row":198,"ts_ms":1747132608000,"snapshot":"false","db":"e_commerce","table":"user_info_sup_msg"},"ts_ms":1747132603683}

        SingleOutputStreamOperator<JSONObject> filterUserInfoSupMsgDS = dataConvertJsonDS.filter(data -> data.getJSONObject("source").getString("table").equals("user_info_sup_msg"));

        //提取user_info表数据,并补充年龄年代星座字段
//        {"birthday":"1999-01-09","uid":"196","decade":1990,"login_name":"rc76n9p25i6","uname":"司马有坚","gender":"M","zodiac_sign":"摩羯座","user_level":"1","phone_num":"13335669677","email":"rc76n9p25i6@gmail.com","ts_ms":1746757081350,"age":26}
//        {"birthday":"2006-03-09","uid":"197","decade":2000,"login_name":"tjcklra6cq2f","uname":"鲁光","gender":"home","zodiac_sign":"双鱼座","user_level":"1","phone_num":"13948471483","email":"tjcklra6cq2f@163.com","ts_ms":1746757082230,"age":19}
//        {"birthday":"2000-01-09","uid":"198","decade":2000,"login_name":"btxahcagy","uname":"戚莲","gender":"F","zodiac_sign":"摩羯座","user_level":"1","phone_num":"13125455788","email":"btxahcagy@yahoo.com.cn","ts_ms":1746757082559,"age":25}
//        {"birthday":"1994-10-09","uid":"199","decade":1990,"login_name":"ohcy5y","uname":"毕永","gender":"home","zodiac_sign":"天秤座","user_level":"2","phone_num":"13555179861","email":"ohcy5y@0355.net","ts_ms":1746757083271,"age":30}

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

        //处理user_info_sup_msg表,提取出需要的字段
//        {"uid":"199","unit_height":"cm","create_ts":1747161408000,"weight":"75","unit_weight":"kg","ts_ms":1747132603683,"height":"159"}
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
//        {"birthday":"2006-03-09","decade":2000,"uname":"鲁光","gender":"home","zodiac_sign":"双鱼座","weight":"47","uid":"197","login_name":"tjcklra6cq2f","unit_height":"cm","user_level":"1","phone_num":"13948471483","unit_weight":"kg","email":"tjcklra6cq2f@163.com","ts_ms":1746757082230,"age":19,"height":"185"}
//        {"birthday":"2000-01-09","decade":2000,"uname":"戚莲","gender":"F","zodiac_sign":"摩羯座","weight":"53","uid":"198","login_name":"btxahcagy","unit_height":"cm","user_level":"1","phone_num":"13125455788","unit_weight":"kg","email":"btxahcagy@yahoo.com.cn","ts_ms":1746757082559,"age":25,"height":"155"}
//        {"birthday":"1994-10-09","decade":1990,"uname":"毕永","gender":"home","zodiac_sign":"天秤座","weight":"75","uid":"199","login_name":"ohcy5y","unit_height":"cm","user_level":"2","phone_num":"13555179861","unit_weight":"kg","email":"ohcy5y@0355.net","ts_ms":1746757083271,"age":30,"height":"159"}

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
//        {"op":"u","before":{"payment_way":"3501","refundable_time":1747390688000,"original_total_amount":17051.0,"order_status":"1001","consignee_tel":"13269938288","trade_body":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Z03女王红 性感冷艳 璀璨金钻哑光唇膏 等5件商品","id":381,"consignee":"东方凤洁","create_time":1746785888000,"coupon_reduce_amount":30.0,"out_trade_no":"853381195519135","total_amount":17021.0,"user_id":121,"province_id":12,"activity_reduce_amount":0.0},"after":{"payment_way":"3501","refundable_time":1747390688000,"original_total_amount":17051.0,"order_status":"1002","consignee_tel":"13269938288","trade_body":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Z03女王红 性感冷艳 璀璨金钻哑光唇膏 等5件商品","id":381,"operate_time":1746785888000,"consignee":"东方凤洁","create_time":1746785888000,"coupon_reduce_amount":30.0,"out_trade_no":"853381195519135","total_amount":17021.0,"user_id":121,"province_id":12,"activity_reduce_amount":0.0},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":1202980,"name":"mysql_binlog_source","row":0,"ts_ms":1746757088000,"snapshot":"false","db":"e_commerce","table":"order_info"},"ts_ms":1746757085350}
//        {"op":"u","before":{"payment_way":"3501","refundable_time":1747296800000,"original_total_amount":69.0,"order_status":"1002","consignee_tel":"13911258321","trade_body":"CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M01醉蔷薇等1件商品","id":134,"operate_time":1746692001000,"consignee":"安广","create_time":1746692000000,"coupon_reduce_amount":0.0,"out_trade_no":"167925515292125","total_amount":69.0,"user_id":102,"province_id":12,"activity_reduce_amount":0.0},"after":{"payment_way":"3501","refundable_time":1747296800000,"original_total_amount":69.0,"order_status":"1004","consignee_tel":"13911258321","trade_body":"CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M01醉蔷薇等1件商品","id":134,"operate_time":1746785888000,"consignee":"安广","create_time":1746692000000,"coupon_reduce_amount":0.0,"out_trade_no":"167925515292125","total_amount":69.0,"user_id":102,"province_id":12,"activity_reduce_amount":0.0},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":1204981,"name":"mysql_binlog_source","row":0,"ts_ms":1746757088000,"snapshot":"false","db":"e_commerce","table":"order_info"},"ts_ms":1746757085456}
//        {"op":"u","before":{"payment_way":"3501","refundable_time":1747390688000,"original_total_amount":26494.0,"order_status":"1001","consignee_tel":"13536559486","trade_body":"Apple iPhone 12 (A2404) 64GB 黑色 支持移动联通电信5G 双卡双待手机等5件商品","id":382,"consignee":"曹桂娣","create_time":1746785888000,"coupon_reduce_amount":0.0,"out_trade_no":"429368819385771","total_amount":25574.1,"user_id":40,"province_id":18,"activity_reduce_amount":919.9},"after":{"payment_way":"3501","refundable_time":1747390688000,"original_total_amount":26494.0,"order_status":"1002","consignee_tel":"13536559486","trade_body":"Apple iPhone 12 (A2404) 64GB 黑色 支持移动联通电信5G 双卡双待手机等5件商品","id":382,"operate_time":1746785888000,"consignee":"曹桂娣","create_time":1746785888000,"coupon_reduce_amount":0.0,"out_trade_no":"429368819385771","total_amount":25574.1,"user_id":40,"province_id":18,"activity_reduce_amount":919.9},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":1206692,"name":"mysql_binlog_source","row":0,"ts_ms":1746757088000,"snapshot":"false","db":"e_commerce","table":"order_info"},"ts_ms":1746757085529}
//        {"op":"u","before":{"payment_way":"3501","refundable_time":1747390671000,"original_total_amount":8197.0,"order_status":"1002","consignee_tel":"13536559486","trade_body":"Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机等1件商品","id":192,"operate_time":1746785873000,"consignee":"曹桂娣","create_time":1746785871000,"coupon_reduce_amount":0.0,"out_trade_no":"589176153599338","total_amount":8197.0,"user_id":40,"province_id":30,"activity_reduce_amount":0.0},"after":{"payment_way":"3501","refundable_time":1747390671000,"original_total_amount":8197.0,"order_status":"1004","consignee_tel":"13536559486","trade_body":"Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机等1件商品","id":192,"operate_time":1746785888000,"consignee":"曹桂娣","create_time":1746785871000,"coupon_reduce_amount":0.0,"out_trade_no":"589176153599338","total_amount":8197.0,"user_id":40,"province_id":30,"activity_reduce_amount":0.0},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":1208180,"name":"mysql_binlog_source","row":0,"ts_ms":1746757088000,"snapshot":"false","db":"e_commerce","table":"order_info"},"ts_ms":1746757085676}
        SingleOutputStreamOperator<JSONObject> filterOrderInfoDS = dataConvertJsonDS.filter(data -> data.getJSONObject("source").getString("table").equals("order_info"));

        //过滤出order_detail表
//        {"op":"c","after":{"sku_num":1,"create_time":1746785888000,"split_coupon_amount":0.0,"sku_id":18,"sku_name":"TCL 75Q10 75英寸 QLED原色量子点电视 安桥音响 AI声控智慧屏 超薄全面屏 MEMC防抖 3+32GB 平板电视","order_price":9199.0,"id":613,"order_id":382,"split_activity_amount":919.9,"split_total_amount":8279.1},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":1198372,"name":"mysql_binlog_source","row":0,"ts_ms":1746757088000,"snapshot":"false","db":"e_commerce","table":"order_detail"},"ts_ms":1746757085294}
//        {"op":"c","after":{"sku_num":1,"create_time":1746785888000,"split_coupon_amount":0.0,"sku_id":35,"sku_name":"华为智慧屏V65i 65英寸 HEGE-560B 4K全面屏智能电视机 多方视频通话 AI升降摄像头 4GB+32GB 星际黑","order_price":5499.0,"id":614,"order_id":382,"split_activity_amount":0.0,"split_total_amount":5499.0},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":1198861,"name":"mysql_binlog_source","row":0,"ts_ms":1746757088000,"snapshot":"false","db":"e_commerce","table":"order_detail"},"ts_ms":1746757085295}
//        {"op":"c","after":{"sku_num":1,"create_time":1746785888000,"split_coupon_amount":0.0,"sku_id":32,"sku_name":"香奈儿（Chanel）女士香水5号香水 粉邂逅柔情淡香水EDT 5号淡香水35ml","order_price":300.0,"id":615,"order_id":382,"split_activity_amount":0.0,"split_total_amount":300.0},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":1199349,"name":"mysql_binlog_source","row":0,"ts_ms":1746757088000,"snapshot":"false","db":"e_commerce","table":"order_detail"},"ts_ms":1746757085298}

        SingleOutputStreamOperator<JSONObject> filterOrderDetailDS = dataConvertJsonDS.filter(data -> data.getJSONObject("source").getString("table").equals("order_detail"));

        //对于order_info进行去重
//        {"op":"u","before":{"payment_way":"3501","refundable_time":1747296800000,"original_total_amount":69.0,"order_status":"1002","consignee_tel":"13911258321","trade_body":"CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M01醉蔷薇等1件商品","id":134,"operate_time":1746692001000,"consignee":"安广","create_time":1746692000000,"coupon_reduce_amount":0.0,"out_trade_no":"167925515292125","total_amount":69.0,"user_id":102,"province_id":12,"activity_reduce_amount":0.0},"after":{"payment_way":"3501","refundable_time":1747296800000,"original_total_amount":69.0,"order_status":"1004","consignee_tel":"13911258321","trade_body":"CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M01醉蔷薇等1件商品","id":134,"operate_time":1746785888000,"consignee":"安广","create_time":1746692000000,"coupon_reduce_amount":0.0,"out_trade_no":"167925515292125","total_amount":69.0,"user_id":102,"province_id":12,"activity_reduce_amount":0.0},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":1204981,"name":"mysql_binlog_source","row":0,"ts_ms":1746757088000,"snapshot":"false","db":"e_commerce","table":"order_info"},"ts_ms":1746757085456}
//        {"op":"u","before":{"payment_way":"3501","refundable_time":1747390688000,"original_total_amount":26494.0,"order_status":"1001","consignee_tel":"13536559486","trade_body":"Apple iPhone 12 (A2404) 64GB 黑色 支持移动联通电信5G 双卡双待手机等5件商品","id":382,"consignee":"曹桂娣","create_time":1746785888000,"coupon_reduce_amount":0.0,"out_trade_no":"429368819385771","total_amount":25574.1,"user_id":40,"province_id":18,"activity_reduce_amount":919.9},"after":{"payment_way":"3501","refundable_time":1747390688000,"original_total_amount":26494.0,"order_status":"1002","consignee_tel":"13536559486","trade_body":"Apple iPhone 12 (A2404) 64GB 黑色 支持移动联通电信5G 双卡双待手机等5件商品","id":382,"operate_time":1746785888000,"consignee":"曹桂娣","create_time":1746785888000,"coupon_reduce_amount":0.0,"out_trade_no":"429368819385771","total_amount":25574.1,"user_id":40,"province_id":18,"activity_reduce_amount":919.9},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":1206692,"name":"mysql_binlog_source","row":0,"ts_ms":1746757088000,"snapshot":"false","db":"e_commerce","table":"order_info"},"ts_ms":1746757085529}
//        {"op":"u","before":{"payment_way":"3501","refundable_time":1747390671000,"original_total_amount":8197.0,"order_status":"1002","consignee_tel":"13536559486","trade_body":"Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机等1件商品","id":192,"operate_time":1746785873000,"consignee":"曹桂娣","create_time":1746785871000,"coupon_reduce_amount":0.0,"out_trade_no":"589176153599338","total_amount":8197.0,"user_id":40,"province_id":30,"activity_reduce_amount":0.0},"after":{"payment_way":"3501","refundable_time":1747390671000,"original_total_amount":8197.0,"order_status":"1004","consignee_tel":"13536559486","trade_body":"Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机等1件商品","id":192,"operate_time":1746785888000,"consignee":"曹桂娣","create_time":1746785871000,"coupon_reduce_amount":0.0,"out_trade_no":"589176153599338","total_amount":8197.0,"user_id":40,"province_id":30,"activity_reduce_amount":0.0},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000025","connector":"mysql","pos":1208180,"name":"mysql_binlog_source","row":0,"ts_ms":1746757088000,"snapshot":"false","db":"e_commerce","table":"order_info"},"ts_ms":1746757085676}
        SingleOutputStreamOperator<JSONObject> processStageOrderInfoDs = filterOrderInfoDS
                .keyBy(data -> "all")
                .process(new ProcessFilterRepeatTsData());





        //过滤掉order_info的脏数据,即我只要最新的order_id的数据,然后与order_detail进行intervalJoin
        //获取到order_info 订单id,用户id,创建时间,order_detail的sku_id,order_price
        //根据sku_id,通过jdbc连接mysql,sku_info的spu_id,spu_info的c3_id,tm_id,连接c3,c2,c1,category_compare_dic2的order_category,base_trademark中的品牌名
        //然后根据use_id分组,把order_category,order_price,品牌名进行计数,获取投入最大的价格和品类和品牌
        //然后进行打分
//        {"payment_way":"3501","refundable_time":1747296800000,"original_total_amount":69.0,"order_status":"1004","consignee_tel":"13911258321","trade_body":"CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M01醉蔷薇等1件商品","id":134,"operate_time":1746785888000,"consignee":"安广","create_time":1746692000000,"coupon_reduce_amount":0.0,"out_trade_no":"167925515292125","total_amount":69.0,"user_id":102,"province_id":12,"activity_reduce_amount":0.0,"ts_ms":"1746757085456"}
//        {"payment_way":"3501","refundable_time":1747390688000,"original_total_amount":26494.0,"order_status":"1002","consignee_tel":"13536559486","trade_body":"Apple iPhone 12 (A2404) 64GB 黑色 支持移动联通电信5G 双卡双待手机等5件商品","id":382,"operate_time":1746785888000,"consignee":"曹桂娣","create_time":1746785888000,"coupon_reduce_amount":0.0,"out_trade_no":"429368819385771","total_amount":25574.1,"user_id":40,"province_id":18,"activity_reduce_amount":919.9,"ts_ms":"1746757085529"}
//        {"payment_way":"3501","refundable_time":1747390671000,"original_total_amount":8197.0,"order_status":"1004","consignee_tel":"13536559486","trade_body":"Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机等1件商品","id":192,"operate_time":1746785888000,"consignee":"曹桂娣","create_time":1746785871000,"coupon_reduce_amount":0.0,"out_trade_no":"589176153599338","total_amount":8197.0,"user_id":40,"province_id":30,"activity_reduce_amount":0.0,"ts_ms":"1746757085676"}

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
                //根据订单id分区,设置状态算子,取到同一订单最新的数据,进行去重
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
//        {"sku_id":"9","order_price":8197.0,"order_id":"381"}
//        {"sku_id":"8","order_price":8197.0,"order_id":"381"}
//        {"sku_id":"8","order_price":8197.0,"order_id":"382"}
//        {"sku_id":"21","order_price":3299.0,"order_id":"382"}

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
        //{"user_id":"40","sku_id":"32","order_price":300.0,"order_time":18,"order_id":"382","ts_ms":"1746757085529"}
//        {"user_id":"40","sku_id":"21","order_price":3299.0,"order_time":18,"order_id":"382","ts_ms":"1746757085529"}
//        {"user_id":"40","sku_id":"8","order_price":8197.0,"order_time":18,"order_id":"382","ts_ms":"1746757085529"}
//        {"user_id":"40","sku_id":"35","order_price":5499.0,"order_time":18,"order_id":"382","ts_ms":"1746757085529"}
//        {"user_id":"40","sku_id":"18","order_price":9199.0,"order_time":18,"order_id":"382","ts_ms":"1746757085529"}
        SingleOutputStreamOperator<JSONObject> orderInfoJoinOrderDetailDS = latestOrderInfoProcessDS.map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        JSONObject result = new JSONObject();
                        result.put("order_id", jsonObject.getString("id"));
                        result.put("order_time", jsonObject.getLongValue("create_time"));
                        result.put("user_id", jsonObject.getString("user_id"));
                        result.put("ts_ms", jsonObject.getString("ts_ms"));
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

        //进行一个汇总的操作,设置一个mapState,键为价格区间,品牌,品类,时间区间,值为map,map的键为具体分类和出现次数
//        {"time_30_34":0.06,"price_18_24":0.12,"category_18_24":0.27,"tm_35_39":0.06,"time_50以上":0.03,"order_time":"晚上","price_30_34":0.06,"tm_name":"索芙特","time_18_24":0.08,"time_25_29":0.07,"tm_50以上":0.02,"category_35_39":0.12,"category_50以上":0.03,"price_50以上":0.015,"category_40_49":0.06,"time_35_39":0.05,"time_40_49":0.04,"category_25_29":0.24,"order_price":"低价商品","tm_30_34":0.1,"tm_18_24":0.18,"price_35_39":0.045,"order_category":"潮流服饰","category_30_34":0.18,"user_id":"198","price_25_29":0.09,"tm_25_29":0.14,"price_40_49":0.03,"tm_40_49":0.04,"ts":1746757083526}
//        {"time_30_34":0.06,"price_18_24":0.015,"category_18_24":0,"tm_35_39":0.06,"time_50以上":0.03,"order_time":"晚上","price_30_34":0.045,"tm_name":"TCL","time_18_24":0.08,"time_25_29":0.07,"tm_50以上":0.02,"category_35_39":0,"category_50以上":0,"price_50以上":0.09,"category_40_49":0,"time_35_39":0.05,"time_40_49":0.04,"category_25_29":0,"order_price":"高价商品","tm_30_34":0.1,"tm_18_24":0.18,"price_35_39":0.06,"order_category":"家居用品","category_30_34":0,"user_id":"197","price_25_29":0.03,"tm_25_29":0.14,"price_40_49":0.075,"tm_40_49":0.04,"ts":1746757083594}
//        {"time_30_34":0.06,"price_18_24":0.03,"category_18_24":0,"tm_35_39":0.06,"time_50以上":0.03,"order_time":"晚上","price_30_34":0.09,"tm_name":"CAREMiLLE","time_18_24":0.08,"time_25_29":0.07,"tm_50以上":0.02,"category_35_39":0,"category_50以上":0,"price_50以上":0.105,"category_40_49":0,"time_35_39":0.05,"time_40_49":0.04,"category_25_29":0,"order_price":"中价商品","tm_30_34":0.1,"tm_18_24":0.18,"price_35_39":0.105,"order_category":"家居用品","category_30_34":0,"user_id":"196","price_25_29":0.06,"tm_25_29":0.14,"price_40_49":0.12,"tm_40_49":0.04,"ts":1746757084040}
//        {"time_30_34":0.06,"price_18_24":0.015,"category_18_24":0,"tm_35_39":0.06,"time_50以上":0.03,"order_time":"晚上","price_30_34":0.045,"tm_name":"小米","time_18_24":0.08,"time_25_29":0.07,"tm_50以上":0.02,"category_35_39":0,"category_50以上":0,"price_50以上":0.09,"category_40_49":0,"time_35_39":0.05,"time_40_49":0.04,"category_25_29":0,"order_price":"高价商品","tm_30_34":0.1,"tm_18_24":0.18,"price_35_39":0.06,"order_category":"家居用品","category_30_34":0,"user_id":"40","price_25_29":0.03,"tm_25_29":0.14,"price_40_49":0.075,"tm_40_49":0.04,"ts":1746757085281}

        SingleOutputStreamOperator<JSONObject> orderLabelDS =
                orderInfoJoinOrderDetailDS
                .keyBy(data -> data.getString("user_id"))
                .process(new OrderValueConvertSetFunc())
                .keyBy(data -> data.getString("user_id"))
                .process(new AggregateOrderLabelFunc())
                        .process(new MapOrderMarkModelFunc());






        //订单标签输出,用户名,品牌名,价格区间,时间区间,品类
//        {"time_30_34":0.05,"price_18_24":0.015,"category_18_24":0,"tm_35_39":0.06,"time_50以上":0.04,"order_time":"下午","price_30_34":0.045,"tm_name":"小米","time_18_24":0.04,"time_25_29":0.05,"tm_50以上":0.02,"category_35_39":0,"category_50以上":0,"price_50以上":0.09,"category_40_49":0,"time_35_39":0.05,"time_40_49":0.05,"category_25_29":0,"order_price":"高价商品","tm_30_34":0.1,"tm_18_24":0.18,"price_35_39":0.06,"order_category":"家居用品","category_30_34":0,"user_id":"40","price_25_29":0.03,"tm_25_29":0.14,"price_40_49":0.075,"tm_40_49":0.04,"ts":1746757083359}
//        orderLabelDS.print();
        //设备评分和关键词评分输出
//        {"device_35_39":0.04,"os":"iOS","device_50":0.02,"search_25_29":0,"ch":"Appstore","pv":3,"device_30_34":0.05,"device_18_24":0.07,"search_50":0,"search_40_49":0,"uid":"76","device_25_29":0.06,"md":"iPhone 13","search_18_24":0,"judge_os":"iOS","search_35_39":0,"device_40_49":0.03,"search_item":"","ba":"iPhone","ts":1746663183000,"search_30_34":0}
//        deviceAndSearchMarkDS.print();
        //六个基础标签输出
        //{"birthday":"2002-05-09","decade":2000,"uname":"赵素","gender":"home","zodiac_sign":"金牛座","weight":"62","uid":"190","login_name":"m52ixggk0","unit_height":"cm","user_level":"1","phone_num":"13548341396","unit_weight":"kg","email":"m52ixggk0@163.net","ts_ms":1746757080315,"age":23,"height":"173"}
//        processIntervalJoinUserInfo6BaseMessageDs.print();

        //订单设置水位线
        SingleOutputStreamOperator<JSONObject> orderWaterDS = orderLabelDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>noWatermarks()
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                }));


        //设备和搜索设置水位线
        SingleOutputStreamOperator<JSONObject> deviceAndSearchWaterDS = deviceAndSearchMarkDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>noWatermarks()
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                }));

        //六个基础标签设置水位线
        SingleOutputStreamOperator<JSONObject> labelWaterDS = processIntervalJoinUserInfo6BaseMessageDs
                .assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>noWatermarks().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts_ms");
                            }
                        }));

        //分组准备join
        //intervalJoin 输出的流带有水位线，但并不继承前一步的分区信息。要与第三个流再次做 intervalJoin，只需要对输出流调用 .keyBy
//        {"birthday":"1999-03-08","decade":1990,"uname":"马炎","gender":"home","shop_age":"50以上","zodiac_sign":"双鱼座","weight":"46","login_name":"1rm3htq","unit_height":"cm","user_id":"15","user_level":"1","phone_num":"13292258292","unit_weight":"kg","email":"1rm3htq@qq.com","age":26,"height":"160"}
//        {"birthday":"1968-02-08","decade":1960,"uname":"方晶妍","gender":"F","shop_age":"18_24","zodiac_sign":"水瓶座","weight":"50","login_name":"ok3hqncd3wx","unit_height":"cm","user_id":"50","user_level":"1","phone_num":"13199299967","unit_weight":"kg","email":"ok3hqncd3wx@0355.net","age":57,"height":"157"}
//        {"birthday":"1968-05-08","decade":1960,"uname":"尉迟倩","gender":"F","shop_age":"18_24","zodiac_sign":"金牛座","weight":"52","login_name":"uz6ce9","unit_height":"cm","user_id":"28","user_level":"3","phone_num":"13146425993","unit_weight":"kg","email":"uz6ce9@msn.com","age":57,"height":"171"}
//        {"birthday":"1968-05-08","decade":1960,"uname":"尉迟倩","gender":"F","shop_age":"18_24","zodiac_sign":"金牛座","weight":"52","login_name":"uz6ce9","unit_height":"cm","user_id":"28","user_level":"1","phone_num":"13146425993","unit_weight":"kg","email":"uz6ce9@msn.com","age":57,"height":"171"}

        SingleOutputStreamOperator<JSONObject> finalJoinDS = orderWaterDS.keyBy(data -> data.getString("user_id"))
                .intervalJoin(deviceAndSearchWaterDS.keyBy(data -> data.getString("uid")))
                .between(Time.days(-1), Time.days(1))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        // 定义年龄段列表
                        String[] ageGroups = {"18_24", "25_29", "30_34", "35_39", "40_49", "50以上"};
                        // 定义需要累加的字段列表，第一组在 jsonObject，后两组在 jsonObject2
                        String[] fields1 = {"time", "price", "category", "tm"};
                        String[] fields2 = {"device", "search"};

                        // 最终结果容器
                        JSONObject orderJoinDeviceAndSearch = new JSONObject();

                        for (String age : ageGroups) {
                            BigDecimal sum = BigDecimal.ZERO;
                            // 累加 jsonObject 中的字段
                            for (String f : fields1) {
                                BigDecimal value = jsonObject.getBigDecimal(f + "_" + age);
                                sum = sum.add(value != null ? value : BigDecimal.ZERO);
                            }
                            // 累加 jsonObject2 中的字段
                            for (String f : fields2) {
                                BigDecimal value = jsonObject2.getBigDecimal(f + "_" + age);
                                sum = sum.add(value != null ? value : BigDecimal.ZERO);
                            }
                            // 放入结果
                            orderJoinDeviceAndSearch.put(age, sum);
                        }
                        //取到每条数据,占比最大的年龄段
                        JSONObject result = new JSONObject();
                        HashMap<String, Double> compareMap = new HashMap<>();
                        compareMap.put("50以上", orderJoinDeviceAndSearch.getDouble("50以上"));
                        compareMap.put("40_49", orderJoinDeviceAndSearch.getDouble("40_49"));
                        compareMap.put("35_39", orderJoinDeviceAndSearch.getDouble("35_39"));
                        compareMap.put("30_34", orderJoinDeviceAndSearch.getDouble("30_34"));
                        compareMap.put("25_29", orderJoinDeviceAndSearch.getDouble("25_29"));
                        compareMap.put("18_24", orderJoinDeviceAndSearch.getDouble("18_24"));
                        Optional<String> maxKey = compareMap.entrySet().stream().max(Map.Entry.comparingByValue())
                                .map(Map.Entry::getKey);
                        String maxAge = maxKey.orElse("unknown");
                        result.put("user_id", jsonObject.getString("user_id"));
                        result.put("shop_age", maxAge);
                        collector.collect(result);
                    }
                })
                //合并第三个流,补充基本标签
                .keyBy(data -> data.getString("user_id"))
                .intervalJoin(labelWaterDS.keyBy(data -> data.getString("uid")))
                .between(Time.days(-1), Time.days(1))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObj1, JSONObject jsonObj2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                        if (jsonObj1.getString("user_id").equals(jsonObj2.getString("uid"))) {
                            JSONObject result = new JSONObject();
                            result.putAll(jsonObj1);
                            jsonObj2.remove("uid");
                            jsonObj2.remove("ts_ms");
                            result.putAll(jsonObj2);
                            collector.collect(result);
                        }

                    }
                })
                //进行去重,完全相同的json去掉
                .keyBy(data -> data.getString("user_id"))
                .process(new ProcessFilterRepeatTsData());
        finalJoinDS.print();


        //导出为csv
//        15,马炎,50以上,1,13292258292,1rm3htq,1rm3htq@qq.com,home,26,1999-03-08,双鱼座,1990,160,cm,46,kg
//        50,方晶妍,18_24,1,13199299967,ok3hqncd3wx,ok3hqncd3wx@0355.net,F,57,1968-02-08,水瓶座,1960,157,cm,50,kg
//        28,尉迟倩,18_24,3,13146425993,uz6ce9,uz6ce9@msn.com,F,57,1968-05-08,金牛座,1960,171,cm,52,kg
//        28,尉迟倩,18_24,1,13146425993,uz6ce9,uz6ce9@msn.com,F,57,1968-05-08,金牛座,1960,171,cm,52,kg

//        SingleOutputStreamOperator<String> json2Csv = finalJoinDS.map(new MapFunction<JSONObject, String>() {
//            @Override
//            public String map(JSONObject jsonObject) throws Exception {
//                String user_id = jsonObject.getString("user_id");
//                String uname = jsonObject.getString("uname");
//                String shop_age = jsonObject.getString("shop_age");
//                String user_level = jsonObject.getString("user_level");
//                String phone_num = jsonObject.getString("phone_num");
//                String login_name = jsonObject.getString("login_name");
//                String email = jsonObject.getString("email");
//                String gender = jsonObject.getString("gender");
//                String age = jsonObject.getString("age");
//                String birthday = jsonObject.getString("birthday");
//                String zodiac_sign = jsonObject.getString("zodiac_sign");
//                String decade = jsonObject.getString("decade");
//                String height = jsonObject.getString("height");
//                String unit_height = jsonObject.getString("unit_height");
//                String weight = jsonObject.getString("weight");
//                String unit_weight = jsonObject.getString("unit_weight");
//
//                String result = user_id + "," + uname + "," + shop_age + "," + user_level + "," + phone_num + "," + login_name + "," + email +
//                        "," + gender + "," + age + "," + birthday + "," + zodiac_sign + "," + decade + "," + height + "," + unit_height + "," + weight + "," + unit_weight;
//
//                return result;
//            }
//        });
//        FileSink<String> sink = FileSink.forRowFormat(
//                new Path("file:///D:/sensitive_words"),
//                new SimpleStringEncoder<String>("UTF-8")
//        ).withRollingPolicy(
//                DefaultRollingPolicy.builder()
//                        .build()
//        ).build();
//        json2Csv.sinkTo(sink);

        env.execute();
    }

}
