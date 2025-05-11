package com.stream.common.utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.TimeUnit;

//keypoint 环境参数设置工具类
public class EnvironmentSettingUtils {

    //keypoint 检查点相关设置
    public static void defaultParameter(StreamExecutionEnvironment env){
        // 开启 checkpoint 支持在 STREAMING 模式下的 FlinkSink 操作
        env.enableCheckpointing(1000*3);
        // 设置状态后端为 RocksDB
        //HashMapStateBackend：将状态保存在 JVM 堆内存中，适合小状态场景；
        //EmbeddedRocksDBStateBackend：将状态保存在嵌入式 RocksDB 本地数据库中，支持非常大的状态并且默认做增量快照；
//        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        // 替换为 HashMapStateBackend（无需 native 库）
        env.setStateBackend(new HashMapStateBackend());
        // 设定语义模式，默认情况是 exactly_once
        CheckpointConfig config = env.getCheckpointConfig();
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置Checkpoint的存储路径
        config.setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://cdh01:8020/flink_point/ck"));
        //设置checkpoint超时时间,默认十分钟
        config.setCheckpointTimeout(10*60*1000);
        // 设定两个 checkpoint 之间的最小时间间隔，防止出现例如状态数据过大导致 checkpoint 执行时间过长，从而导致 checkpoint 积压过多，
        // 最终 Flink 应用密切触发 checkpoint 操作，会占用大量计算资源而影响整个应用的性能
        config.setMinPauseBetweenCheckpoints(500);
        // 默认情况下，只有一个检查点可以运行
        // 根据用户指定的数量可以同时触发多个 checkpoint，从而提升 checkpoint 整体的效率
        config.setMaxConcurrentCheckpoints(1);
        //Flink 在作业被 取消（cancellation） 时，保留 已经外部化（externalized）的检查点数据
        config.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置可以允许的 checkpoint 失败数
        config.setTolerableCheckpointFailureNumber(3);
        //** 重启策略相关 *//*
        // 重启3次，每次失败后等待10000毫秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000L));
        // 在5分钟内，只能重启5次，每次失败后最少需要等待10秒
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10,TimeUnit.SECONDS)));
    }

    //keypoint s3a相关设置
    public static Configuration getMinioConfiguration(){
        Configuration configuration = new Configuration();
        // 用于设置 AWS 的 Access Key ID，即用于唯一标识调用者身份的公钥部分
        configuration.set("fs.s3a.access.key", "");
        // 用于设置 AWS 的 Secret Access Key，即与 Access Key 配对的私钥部分
        configuration.set("fs.s3a.secret.key", "");

        return configuration;
    }
}
