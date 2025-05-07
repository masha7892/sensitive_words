package com.realtime.func;

import com.realtime.util.SensitiveWordsUtils;
import com.stream.common.utils.ConfigUtils;
import lombok.SneakyThrows;
import sun.reflect.misc.ConstructorUtil;

import javax.security.auth.login.Configuration;
import java.util.ArrayList;

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
        //指定执行用户
        System.setProperty("HADOOP_USER_NAME","root");

    }

}
