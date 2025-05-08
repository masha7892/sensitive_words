package com.stream.common.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;

//keypoint 构建基于字符串序列化的Kafka属性
public class KafkaUtils {

    //构建 Kafka 数据源
    public static KafkaSource<String> buildKafkaSource(String bootServerList, String kafkaTopic, String group, OffsetsInitializer offset){
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootServerList)
                .setTopics(kafkaTopic)
                .setGroupId(group)
                .setStartingOffsets(offset)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    //构建 带有安全的反序列化器的Kafka 数据源
    public static KafkaSource<String> buildKafkaSecureSource(String bootServerList,String kafkaTopic,String group,OffsetsInitializer offset){
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootServerList)
                .setTopics(kafkaTopic)
                .setGroupId(group)
                .setStartingOffsets(offset)
                .setValueOnlyDeserializer(new SafeStringDeserializationSchema())
                .build();
    }


    //自定义安全的反序列化器
    public static class SafeStringDeserializationSchema implements DeserializationSchema<String> {

        @Override
        public String deserialize(byte[] bytes) throws IOException {
            if(bytes == null){
                return null;
            }
            return new String(bytes);
        }

        @Override
        public boolean isEndOfStream(String s) {
            return false;
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }
}
